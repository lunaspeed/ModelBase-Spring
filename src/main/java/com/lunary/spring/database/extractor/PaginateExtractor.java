
package com.lunary.spring.database.extractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import com.lunary.database.BasePageContainer;
import com.lunary.database.PageContainer;

public class PaginateExtractor<T> implements ResultSetExtractor<PageContainer<T>> {

  private final RowMapper<T> rowMapper;
  private final int rowsPerPage;
  private final int startingRow;
  private final int endingRow;
  
  public PaginateExtractor(RowMapper<T> rowMapper, int page, int rowsPerPage) {
    this.rowMapper = rowMapper;
    this.rowsPerPage = rowsPerPage;
    this.startingRow = ((page - 1) * rowsPerPage) + 1;
    this.endingRow = this.startingRow + rowsPerPage;
  }
  
  @Override
  public PageContainer<T> extractData(ResultSet rs) throws SQLException, DataAccessException {
    
    PageContainer<T> container = new BasePageContainer<T>();
    List<T> rows = new ArrayList<T>(rowsPerPage);
    int cnt = 0;
    while (rs.next()) {
        cnt += 1;
        if (cnt >= startingRow && cnt <= endingRow) {
            rows.add(rowMapper.mapRow(rs, cnt));
        }
    }
    container.setRows(rows);
    container.setTotalRows(cnt);
    int totalPages = cnt / rowsPerPage;
    if (cnt % rowsPerPage != 0) {
        totalPages += 1;
    }
    container.setTotalPages(totalPages);
    return container;
  }

}
