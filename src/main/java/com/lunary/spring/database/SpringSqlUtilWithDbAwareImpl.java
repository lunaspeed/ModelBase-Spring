package com.lunary.spring.database;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.lunary.database.BasePageContainer;
import com.lunary.database.ColumnMapper;
import com.lunary.database.DatabaseType;
import com.lunary.database.PageContainer;
import com.lunary.database.PaginateSqlTemplate;
import com.lunary.database.TopSqlTemplate;
import com.lunary.database.impl.Db2PaginateSqlTemplate;
import com.lunary.database.impl.Db2TopSqlTemplate;
import com.lunary.database.impl.MSSqlPaginateSqlTemplate;
import com.lunary.database.impl.MSSqlTopSqlTemplate;
import com.lunary.database.impl.MySqlPaginateSqlTemplate;
import com.lunary.database.impl.MySqlTopSqlTemplate;
import com.lunary.database.impl.OracleTopSqlTemplate;

public class SpringSqlUtilWithDbAwareImpl extends SpringSqlUtil {

  private final Logger logger = LoggerFactory.getLogger(SpringSqlUtilWithDbAwareImpl.class);
  private final PaginateSqlTemplate paginateTemplate;
  private final TopSqlTemplate topTemplate;
  private final Pattern orderByPattern = Pattern.compile(" ORDER BY ", Pattern.CASE_INSENSITIVE);

  /**
   * Consturctor
   * 
   * @param jdbcTemplate
   * @param queryRunner
   * @param databaseType
   *          MSSQL, DB2, ORACLE, or SYBASE {@see com.lunary.database.DatabaseType}
   * @param columnMapper
   * @throws IllegalArgumentException
   *           databaseType is not MSSQL, DB2, ORACLE, or SYBASE {@see com.lunary.database.DatabaseType}
   * @throws NullPointerException
   *           if any argument is null
   */
  public SpringSqlUtilWithDbAwareImpl(JdbcTemplate jdbcTemplate, DataSource dataSource, String databaseType, ColumnMapper columnMapper) throws IllegalArgumentException, NullPointerException {

    this(jdbcTemplate, dataSource, Enum.valueOf(DatabaseType.class, databaseType.toUpperCase()), columnMapper);
  }

  /**
   * Consturctor
   * 
   * @param jdbcTemplate
   * @param queryRunner
   * @param databaseType
   * @param columnMapper
   * @throws NullPointerException
   *           if jdbcTemplate or databaseType is null
   */
  public SpringSqlUtilWithDbAwareImpl(JdbcTemplate jdbcTemplate, DataSource dataSource, DatabaseType databaseType, ColumnMapper columnMapper) throws NullPointerException {

    super(jdbcTemplate, dataSource, columnMapper);

    //this.databaseType = databaseType;

    if(databaseType == DatabaseType.MYSQL) {
      paginateTemplate = new MySqlPaginateSqlTemplate();
      topTemplate = new MySqlTopSqlTemplate();
    }
    else if (databaseType == DatabaseType.DB2) {
      paginateTemplate = new Db2PaginateSqlTemplate();
      topTemplate = new Db2TopSqlTemplate();
    }
    else if (databaseType == DatabaseType.MSSQL95 || databaseType == DatabaseType.MSSQL2008) {//TODO need better paginateTemplate for MSSQL 2008 due to new paginate sytax
      paginateTemplate = new MSSqlPaginateSqlTemplate();
      topTemplate = new MSSqlTopSqlTemplate();
    }
    else if (databaseType == DatabaseType.ORACLE) {
      //TODO implement OraclePaginateSqlTemplate
      paginateTemplate = null;
      topTemplate = new OracleTopSqlTemplate();
    }
    else if (databaseType == DatabaseType.SYBASE) {
      //currently Sybase does not support any good pagination mechanism 
      paginateTemplate = null;
      topTemplate = new MSSqlTopSqlTemplate();
    }
    else {
      logger.warn("No PaginateSqlTempate available for databaseType: " + databaseType + ". Default method will be used for pagination queries.");
      paginateTemplate = null;
      topTemplate = null;
    }

    if (paginateTemplate != null) {
      logger.info("PaginateSqlTempate: " + paginateTemplate.getClass() + " will be used for pagination queries.");
    }
    if (topTemplate != null) {
      logger.info("TopSqlTemplate: " + topTemplate.getClass() + " will be used for top queries.");
    }
  }
  
  public SpringSqlUtilWithDbAwareImpl(JdbcTemplate jdbcTemplate, DataSource dataSource, PaginateSqlTemplate paginateTemplate, TopSqlTemplate topTemplate, ColumnMapper columnMapper) throws IllegalArgumentException, NullPointerException {
    
    super(jdbcTemplate, dataSource, columnMapper);
    this.paginateTemplate = paginateTemplate;
    this.topTemplate = topTemplate;
        
  }

  @Override
  public <E> PageContainer<E> findWithPagination(String sql, int page, int rowsPerPage, Class<E> clazz, Object... params) {

    if (page <= 0) {
      page = 1;
    }
    PageContainer<E> pc;

    if (paginateTemplate != null) {
      String orderByColumns = getOrderByColumns(sql);
      sql = trimOrderBy(sql);
      String cntSql = paginateTemplate.formatCountSql(sql);
      logger.debug("Count Sql: {}", cntSql);
      
      int cnt = findCount(cntSql, params);
      sql = paginateTemplate.formatPaginateSql(sql, orderByColumns, page, rowsPerPage);
      logger.debug("Paginate Sql: {}", sql);
      
      List<E> list = this.find(sql, clazz, params);
      pc = new BasePageContainer<E>();
      pc.setRows(list);
      setupPageContainer(pc, cnt, rowsPerPage);
    }
    else {
      pc = super.findWithPagination(sql, page, rowsPerPage, clazz, params);
    }
    return pc;
  }

  @Override
  public PageContainer<Map<String, Object>> findWithPaginationMap(String sql, int page, int rowsPerPage, Object... params) {

    if (page <= 0) {
      page = 1;
    }
    PageContainer<Map<String, Object>> pc;

    if (paginateTemplate != null) {

      String orderByColumns = getOrderByColumns(sql);
      sql = trimOrderBy(sql);
      String cntSql = paginateTemplate.formatCountSql(sql);
      logger.debug("Count Sql: {}", cntSql);

      int cnt = findCount(cntSql, params);
      sql = paginateTemplate.formatPaginateSql(sql, orderByColumns, page, rowsPerPage);
      logger.debug("Paginate Sql: {}", sql);
      
      List<Map<String, Object>> list = this.findWithMap(sql, params);
      pc = new BasePageContainer<Map<String, Object>>();
      pc.setRows(list);
      setupPageContainer(pc, cnt, rowsPerPage);
    }
    else {
      pc = super.findWithPaginationMap(sql, page, rowsPerPage, params);
    }
    return pc;
  }

  private <E> void setupPageContainer(PageContainer<E> pc, int totalRows, int rowsPerPage) {

    pc.setTotalRows(totalRows);
    int totalPages = totalRows / rowsPerPage;
    if (totalRows % rowsPerPage != 0) totalPages += 1;
    pc.setTotalPages(totalPages);
  }

  private String trimOrderBy(String sql) {

    String[] sqlChunk = orderByPattern.split(sql);
    if (sqlChunk.length > 1) {

      StringBuilder sqlBuilder = new StringBuilder();
      int i = 0;
      int l = sqlChunk.length - 1;
      for (String s : sqlChunk) {
        if (i < l) {
          if (i > 0) {
            sqlBuilder.append(" ORDER BY ");
          }
          sqlBuilder.append(s);
        }
        ++i;
      }
      sql = sqlBuilder.toString();
    }
    return sql;
  }

  private String getOrderByColumns(String sql) {

    String orderByColumnName = "1 ";
    String[] sqlChunk = orderByPattern.split(sql);
    if (sqlChunk.length > 1) {
      orderByColumnName = sqlChunk[sqlChunk.length - 1];
    }
    return orderByColumnName;
  }

  @Override
  public <E> List<E> findTop(String sql, int top, Class<E> clazz, Object... params) {

    if (topTemplate != null) {
      sql = topTemplate.formatTopSql(sql, top);
    }
    logger.debug("top sql: {}", sql);
    return super.findTop(sql, top, clazz, params);
  }

  @Override
  public List<Map<String, Object>> findTopWithMap(String sql, int top, Object... params) {

    if (topTemplate != null) {
      sql = topTemplate.formatTopSql(sql, top);
    }
    logger.debug("top sql: {}", sql);
    return super.findTopWithMap(sql, top, params);
  }
}
