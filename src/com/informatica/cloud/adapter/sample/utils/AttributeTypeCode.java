package com.informatica.cloud.adapter.sample.utils;

/**
 * @author anair
 *
 */
public enum AttributeTypeCode {
	STRING("String",1),
	INTEGER("Integer",2),
	DOUBLE("Double",3),
	BOOLEAN("Boolean",4),
	DATETIME("DateTime",5),
	DATE("Date",6),
	DECIMAL("Decimal",7),
	BINARY("Byte",8),
	TIME("Time",9),
	SHORT("Short", 10),
	LONG("Long", 11),
	BIGINT("BigInteger", 12),
	FLOAT("Float", 13),
	BIGDECIMAL("BigDecimal", 14);

    private String dataTypeName;
    private  int dataTypeId;

    AttributeTypeCode(String dataTypeName, int dataTypeId) {
        this.dataTypeName = dataTypeName;
        this.dataTypeId = dataTypeId;
    }
    public String getDataTypeName() {
        return this.dataTypeName;
    }
  
    public int getDataTypeId() {
        return this.dataTypeId;
    }
    
    public static AttributeTypeCode fromValue(String value) {
        for(AttributeTypeCode c : AttributeTypeCode.values()) {
              if(c.getDataTypeName().equals(value)) {
                    return c;
              }
        }
        throw new IllegalArgumentException(value);
  }

	
}
