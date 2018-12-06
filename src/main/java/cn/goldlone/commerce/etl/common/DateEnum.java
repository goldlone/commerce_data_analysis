package cn.goldlone.commerce.etl.common;

/**
 * 日期枚举类型
 * @author lyd
 *
 */
public enum DateEnum {
	
	YEAR("year"),
	SEASON("season"),
	MONTH("month"),
	WEEK("week"),
	DAY("day"),
	HOUR("hour");
	
	public final String name; //名字    //注意需要public修饰

	private DateEnum(String name) {
		this.name = name;
	}
	//-----------------以上三者必先要有-------------------
	
	/**
	 * 根据属性name的值获取对应的type
	 * @param name
	 * @return
	 */
	public static DateEnum valueOfName(String name){
		//循环枚举属性
		for (DateEnum type : values()) {
			if(type.name.equals(name)){
				return type;
			}
		}
		return null;
	}
}
