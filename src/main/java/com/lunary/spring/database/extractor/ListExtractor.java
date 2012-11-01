
package com.lunary.spring.database.extractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

public class ListExtractor<T> implements ResultSetExtractor<List<T>> {

  private final RowMapper<T> rowMapper;

  private final int topRows;


  /**
   * Create a new RowMapperResultSetExtractor.
   * @param rowMapper the RowMapper which creates an object for each row
   */
  public ListExtractor(RowMapper<T> rowMapper) {
    this(rowMapper, 0);
  }

  /**
   * Create a new RowMapperResultSetExtractor.
   * @param rowMapper the RowMapper which creates an object for each row
   * @param topRows the number of rows to get, all extra will be ignored
   */
  public ListExtractor(RowMapper<T> rowMapper, int topRows) {
    Assert.notNull(rowMapper, "RowMapper is required");
    this.rowMapper = rowMapper;
    this.topRows = topRows;
  }

  @Override
  public List<T> extractData(ResultSet rs) throws SQLException {
    List<T> results = (this.topRows > 0 ? new ArrayList<T>(this.topRows) : new ArrayList<T>());
    int rowNum = 0;
    while (rs.next() && (topRows <= 0 || topRows > rowNum)) {
      results.add(this.rowMapper.mapRow(rs, rowNum++));
    }
    return results;
  }
}
