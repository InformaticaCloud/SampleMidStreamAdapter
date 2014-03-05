package com.informatica.cloud.adapter.sample.transform;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.informatica.cloud.adapter.sample.SampleConstants;
import com.informatica.cloud.adapter.sample.connection.SampleConnection;
import com.informatica.cloud.adapter.sample.plugin.SamplePlugin;
import com.informatica.cloud.api.adapter.annotation.Deinit;
import com.informatica.cloud.api.adapter.common.OperationContext;
import com.informatica.cloud.api.adapter.connection.ConnectionFailedException;
import com.informatica.cloud.api.adapter.metadata.Field;
import com.informatica.cloud.api.adapter.metadata.RecordInfo;
import com.informatica.cloud.api.adapter.plugin.PluginVersion;
import com.informatica.cloud.api.adapter.runtime.IWrite2;
import com.informatica.cloud.api.adapter.runtime.SessionStats;
import com.informatica.cloud.api.adapter.runtime.SessionStats.Operation;
import com.informatica.cloud.api.adapter.runtime.exception.DataConversionException;
import com.informatica.cloud.api.adapter.runtime.exception.FatalRuntimeException;
import com.informatica.cloud.api.adapter.runtime.exception.InitializationException;
import com.informatica.cloud.api.adapter.runtime.exception.ReflectiveOperationException;
import com.informatica.cloud.api.adapter.runtime.exception.WriteException;
import com.informatica.cloud.api.adapter.runtime.utils.IInputDataBuffer;
import com.informatica.cloud.api.adapter.runtime.utils.IOutputDataBuffer;
import com.informatica.cloud.api.adapter.typesystem.JavaDataType;
import com.sample.wsproxy.Record;
import com.sample.wsproxy.Writeresponse;

public class SampleMidStreamWrite implements IWrite2 {

	private Map<String,Map<String, String>> recordAttributes = new HashMap<String, Map<String,String>>();
	private Map<String, String> transformOperAttrs = new HashMap<String, String>();
	private List<Field> inputFieldList = new ArrayList<Field>();
	private RecordInfo primaryRecordInfo;
	private List<Field> outputFieldList = new ArrayList<Field>(); 
	private Object[] rowData;
	private com.sample.wsproxy.SampleData port;
	private SamplePlugin samplePlugin;
	private SampleConnection sampleConn;
	private List<Field> errorOutputFieldList = new ArrayList<Field>();
	private IOutputDataBuffer errorOutputBuffer;
	private IOutputDataBuffer outputBuffer;
	private final List<SessionStats> cumulativeSessionStats = new ArrayList<SessionStats>();
	private final SessionStats insertStats = new SessionStats(Operation.INSERT, 0, 0);
	private final SessionStats deleteStats = new SessionStats(Operation.DELETE, 0, 0);
	private final SessionStats updateStats = new SessionStats(Operation.UPDATE, 0, 0);
	private final SessionStats upsertStats = new SessionStats(Operation.UPSERT, 0, 0);
	private Object[] errorRowData;
	
	public SampleMidStreamWrite(SamplePlugin samplePlugin, SampleConnection conn) {
		this.samplePlugin = samplePlugin;
		this.sampleConn = conn;
		cumulativeSessionStats.add(insertStats);
		cumulativeSessionStats.add(deleteStats);
		cumulativeSessionStats.add(updateStats);
		cumulativeSessionStats.add(upsertStats);
	}

	@Override
	public void insert(IInputDataBuffer inputDataBuffer) throws ConnectionFailedException,
			ReflectiveOperationException, WriteException,
			DataConversionException, FatalRuntimeException {
		executeOperation(Operation.INSERT, inputDataBuffer);
	}
	
	private void executeOperation(Operation operation, IInputDataBuffer inputDataBuffer) {

		init();
		try {
			int numProcessedRows = 0;
			int successRowCount = 0;
			while(inputDataBuffer.hasMoreRows()) {
			    numProcessedRows++;
				Object[] data = inputDataBuffer.getData();
			    Record primaryRecordInstance = createRecordInstance(data);
			    Writeresponse status = null;
			    switch (operation) {
			    case INSERT : 
			    	status = port.insert(primaryRecordInstance);
			    	break;
			    case UPDATE :
			    	status = port.update(primaryRecordInstance);
			    	break;
			    case DELETE :
			    	status = port.delete(primaryRecordInstance);
			    	break;
			    case UPSERT :
			    default : break;
			    
			    }
				if (status != null && status.getInternalId() != null) {
					successRowCount++;
					populateOutputBuffer(status.getInternalId(), data);
				} else {
					populateErrorOutputBuffer(data);
				}
			}
			switch (operation) {
			case INSERT : 
				insertStats.incrementProcessedRowsCount(numProcessedRows);
				insertStats.incrementSuccessRowsCount(successRowCount);
		    	break;
		    case UPDATE :
		    	updateStats.incrementProcessedRowsCount(numProcessedRows);
		    	updateStats.incrementSuccessRowsCount(successRowCount);
		    	break;
		    case DELETE :
		    	deleteStats.incrementProcessedRowsCount(numProcessedRows);
		    	deleteStats.incrementSuccessRowsCount(successRowCount);
		    	break;
		    case UPSERT :
		    default : break;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private Record createRecordInstance(Object[] data)
			throws Exception {
		String primaryRecordClassName = recordAttributes.get(
				primaryRecordInfo.getRecordName()).get(
				SampleConstants.CLASS_NAME);
		Class<?> primaryClass = Class.forName(primaryRecordClassName);
		Method[] primaryRecordMethods = primaryClass.getMethods();
		Object primaryRecordInstance = primaryClass.newInstance();
		int j = 0;
		for (Field aField : inputFieldList) {
			Object value = data[j];
			if (aField.getContainingRecord().getRecordName()
					.equals(primaryRecordInfo.getRecordName())) {
				for (Method aMethod : primaryRecordMethods) {
					if (aMethod.getName().equalsIgnoreCase(
							"set" + aField.getDisplayName())) {
						JavaDataType javaDT = aField.getJavaDatatype();
						switch (javaDT) {
						case JAVA_STRING:
							aMethod.invoke(primaryRecordInstance,
									(String) value);
							break;
						case JAVA_INTEGER:
							aMethod.invoke(primaryRecordInstance,
									(Integer) value);
							break;
						case JAVA_DOUBLE:
							aMethod.invoke(primaryRecordInstance,
									(Double) value);
							break;
						case JAVA_TIMESTAMP:
							XMLGregorianCalendar xmlGregDate = null;
							if (value != null) {
								GregorianCalendar c = new GregorianCalendar();
								Timestamp ts = (Timestamp) value;
								c.setTimeInMillis(ts.getTime());
								xmlGregDate = DatatypeFactory.newInstance()
										.newXMLGregorianCalendar(c);
							}
							aMethod.invoke(primaryRecordInstance, xmlGregDate);

							break;
						case JAVA_BOOLEAN:
							aMethod.invoke(primaryRecordInstance,
									(Boolean) value);
							break;
						case JAVA_BIGDECIMAL:
							aMethod.invoke(primaryRecordInstance,
									(BigDecimal) value);
							break;
						case JAVA_PRIMITIVE_BYTEARRAY:
							byte[] bytes = (byte[]) value;
							aMethod.invoke(primaryRecordInstance, bytes);
							break;
						case JAVA_SHORT:
							aMethod.invoke(primaryRecordInstance, (Short) value);
							break;
						case JAVA_LONG:
							aMethod.invoke(primaryRecordInstance, (Long) value);
							break;
						case JAVA_BIGINTEGER:
							aMethod.invoke(primaryRecordInstance,
									(BigInteger) value);
							break;
						case JAVA_FLOAT:
							aMethod.invoke(primaryRecordInstance, (Float) value);
							break;
						}
						break;
					}
				}
			}
			j++;
		}
		
		return (Record) primaryRecordInstance;
	}
	
	private void populateOutputBuffer(String internalId, Object[] data) throws DataConversionException, FatalRuntimeException {
		clearRowData(rowData);
		rowData[0] = internalId;
		if (samplePlugin.getContext() == OperationContext.WRITE) {
			int k = 1;
			for (Object value : data) {
				rowData[k] = value;
				k++;
			}
		}
		outputBuffer.setData(rowData);
	}
	
	private void populateErrorOutputBuffer(Object[] data) throws DataConversionException, FatalRuntimeException {
		clearRowData(errorRowData);
		errorRowData[0] = "Error in Operation";
		int k = 1;
		for (Object value : data) {
			errorRowData[k] = value;
			k++;
		}
		errorOutputBuffer.setData(errorRowData);
	}

	@Override
	public void setErrorOutputFieldList(List<Field> errorOutputFieldList) {
		this.errorOutputFieldList.addAll(errorOutputFieldList);
		errorRowData = new Object[errorOutputFieldList.size()];
		
	}

	@Override
	public void setOutputFieldList(List<Field> fieldList) {
		this.outputFieldList.addAll(fieldList);
		rowData = new Object[this.outputFieldList.size()];
		
	}

	@Override
	public void setChildRecords(List<RecordInfo> arg0) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void setInputFieldList(List<Field> fieldList) {
		this.inputFieldList.addAll(fieldList);
		
	}

	@Override
	public void setPrimaryRecord(RecordInfo primaryRecordInfo) {
		this.primaryRecordInfo = primaryRecordInfo;
		
	}

	@Override
	public void initializeAndValidate() throws InitializationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setOperationAttributes(Map<String, String> roAttribs) {
		this.transformOperAttrs.putAll(roAttribs);
	}

	@Override
	public void setRecordAttributes(RecordInfo recordInfo, Map<String, String> srcDesigntimeAttribs) {
		this.recordAttributes.put(recordInfo.getRecordName(), srcDesigntimeAttribs);
		
	}

	@Override
	public void setMetadataVersion(PluginVersion arg0) {
		// TODO Auto-generated method stub
		
	}
	
	private void init() {
		com.sample.wsproxy.SampleDataService service = new com.sample.wsproxy.SampleDataService();
		port = service.getSampleDataPort();
		
	}
	
	private void clearRowData(Object[] data) {
		for(int i = 0; i < data.length ; i++) {
			data[i] = null;
		}
		
	}
	
	@Override
	public void delete(IInputDataBuffer inputDataBuffer) throws ConnectionFailedException,
			ReflectiveOperationException, WriteException,
			DataConversionException, FatalRuntimeException {
		executeOperation(Operation.DELETE, inputDataBuffer);
		
	}

	@Override
	public void setErrorGroupBuffer(IOutputDataBuffer errorOutputBuffer) {
		this.errorOutputBuffer = errorOutputBuffer;
		
	}

	@Override
	public void setOutputGroupBuffer(IOutputDataBuffer outputBuffer) {
		this.outputBuffer = outputBuffer;
		
	}

	@Override
	public void update(IInputDataBuffer inputDataBuffer) throws ConnectionFailedException,
			ReflectiveOperationException, WriteException,
			DataConversionException, FatalRuntimeException {
		executeOperation(Operation.UPDATE, inputDataBuffer);
	}

	@Override
	public void upsert(IInputDataBuffer arg0) throws ConnectionFailedException,
			ReflectiveOperationException, WriteException,
			DataConversionException, FatalRuntimeException {
		// TODO Auto-generated method stub
		
	}
	
	@Deinit
	@Override
	public List<SessionStats> deinit() {
	    return cumulativeSessionStats;
	}

	@Override
	public List<SessionStats> getCumulativeOperationStats() {
		return cumulativeSessionStats;
	}

}
