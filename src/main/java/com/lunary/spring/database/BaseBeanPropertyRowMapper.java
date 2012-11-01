
package com.lunary.spring.database;

import java.beans.PropertyDescriptor;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.lunary.database.ColumnMapper;

public class BaseBeanPropertyRowMapper<T> extends BeanPropertyRowMapper<T> {

  private final ColumnMapper columnMapper;
  
  public BaseBeanPropertyRowMapper(Class<T> mappedClass, ColumnMapper columnMapper) {
    super(mappedClass);
    this.columnMapper = columnMapper;
  }
  
  @Override
  protected Object getColumnValue(ResultSet rs, int index, PropertyDescriptor pd) throws SQLException {
    
    Object value = columnMapper.toObject(rs, index, pd.getPropertyType());
    if(value == ColumnMapper.UNKNOWN) {
      value = super.getColumnValue(rs, index, pd);
    }
    return value;
  }
}
