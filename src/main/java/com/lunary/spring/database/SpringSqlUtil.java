package com.lunary.spring.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import com.lunary.database.ColumnMapper;
import com.lunary.database.PageContainer;
import com.lunary.database.SqlUtil;
import com.lunary.database.exception.DatabaseException;
import com.lunary.database.util.StatementUtil;
import com.lunary.database.util.StatementUtil.SqlStatement;
import com.lunary.database.util.TableEntityUtil;
import com.lunary.model.IdKeyedTableEntity;
import com.lunary.model.TableEntity;
import com.lunary.spring.database.extractor.ListExtractor;
import com.lunary.spring.database.extractor.PaginateExtractor;
import com.lunary.util.CollectionUtil;
import com.lunary.util.factory.Factory;
/**
 * <pre>
 * This implementation mainly uses JdbcTemplate mechanism.
 * </pre>
 * 
 * @see org.springframework.jdbc.core.JdbcTemplate
 * @author Steven
 * 
 */
public class SpringSqlUtil implements SqlUtil {

  private final String autoGenerateColumnName = IdKeyedTableEntity.ID;
  private final JdbcTemplate jdbcTemplate;

  private final ConcurrentMap<String, SimpleJdbcInsert> insertMap = new ConcurrentHashMap<String, SimpleJdbcInsert>();
  private Factory<SimpleJdbcInsert> insertFactory;
  private final ColumnMapper columnMapper;

  public SpringSqlUtil(final JdbcTemplate jdbcTemplate, DataSource dataSource, ColumnMapper columnMapper) throws NullPointerException {
    
    //super(dataSource, new BasicRowProcessor(new SpringBeanProcessor(lobHandler)));
    if (jdbcTemplate == null) {
      throw new NullPointerException("jdbcTemplate cannot be null");
    }
    this.columnMapper = columnMapper;
    this.jdbcTemplate = jdbcTemplate;
    
    this.insertFactory = new Factory<SimpleJdbcInsert>() {

      @Override
      public SimpleJdbcInsert create(Object... objects) {

        String tableName = (String) objects[0];
        SimpleJdbcInsert sji = new SimpleJdbcInsert(jdbcTemplate).withTableName(tableName).usingGeneratedKeyColumns(autoGenerateColumnName);

        //sji.compile();
        return sji;
      }
    };
  }
  
  public SpringSqlUtil(JdbcTemplate jdbcTemplate, DataSource dataSource) {
    this(jdbcTemplate, dataSource, new SpringColumnMapper());
  }

  protected JdbcTemplate getJdbcTemplate() {
    return jdbcTemplate;
  }

  protected ColumnMapper getColumnMapper() {
    return columnMapper;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public int update(String sql, Object... params) {

    convertParams(params);
    return updateWithoutParamCheck(sql, params);
  }

  private int updateWithoutParamCheck(String sql, Object... params) {
    
    try {
      return jdbcTemplate.update(sql, params);
    }
    catch (DataAccessException e){
      throw translateException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int delete(TableEntity entity) {

    SqlStatement sset = StatementUtil.buildDeleteStatement(entity);

    return this.update(sset.getSql(), sset.getParams());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int insert(TableEntity entity) {

    String tableName = entity.getTableName();
    if(tableName == null) throw new NullPointerException("TableEntity tableName cannot be null.");
    
    SimpleJdbcInsert insert = CollectionUtil.getFromConcurrentMap(insertMap, tableName, insertFactory);
    Map<String, Object> params = TableEntityUtil.convert(entity);

    int cnt = 0;
    if(entity instanceof IdKeyedTableEntity) {
      try {
        // insert.usingColumns((String[]) params.keySet().toArray());
        Number id = insert.executeAndReturnKey(params);
        ((IdKeyedTableEntity) entity).setId(id.longValue());
        cnt = 1;
      }
      finally {
        // Currently SimpleJdbcInsert doesn't call
        // StatementCreatorUtils.cleanupParameters() automatically
        StatementCreatorUtils.cleanupParameters(params.values());
      }
    }
    else {
      cnt = insert.execute(params);
    }
    return cnt;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int update(TableEntity entity) {

    SqlStatement sset = StatementUtil.buildPreparedUpdateStatement(entity, false);
    int cnt = this.updateWithoutParamCheck(sset.getSql(), sset.getParams());

    return cnt;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int updateWithNull(TableEntity entity) {

    SqlStatement sset = StatementUtil.buildPreparedUpdateStatement(entity, true);
    return this.updateWithoutParamCheck(sset.getSql(), sset.getParams());
  }

  @Override
  public <E> List<E> find(String sql, Class<E> clazz, Object... params) {

    ResultSetExtractor<List<E>> rse = new ListExtractor<E>(new BaseBeanPropertyRowMapper<E>(clazz, columnMapper));
    return query(sql, rse, params);
  }

  @Override
  public int findCount(String sql, Object... params) {
    try {
      convert(params);
      return jdbcTemplate.queryForInt(sql, params);
    }
    catch (DataAccessException e) {
      throw translateException(e);
    }
  }

  @Override
  public boolean exists(String fromSql, Object... params) {
    return findTopWithMap("SELECT 1 " + fromSql, 1, params).size() > 0;
  }

  @Override
  public <E> E findOne(String sql, Class<E> clazz, Object... params) {
    try {
      convert(params);
      return jdbcTemplate.queryForObject(sql, params, new BaseBeanPropertyRowMapper<E>(clazz, columnMapper));
    }
    catch (DataAccessException e) {
      throw translateException(e);
    }
  }

  @Override
  public Map<String, Object> findOneWithMap(String sql, Object... params) {
    try {
      convert(params);
      return jdbcTemplate.queryForMap(sql, params);
    }
    catch (DataAccessException e) {
      throw translateException(e);
    }
  }

  @Override
  public <E> List<E> findTop(String sql, int top, Class<E> clazz, Object... params) {
    ResultSetExtractor<List<E>> rse = new ListExtractor<E>(new BaseBeanPropertyRowMapper<E>(clazz, columnMapper), top);
    return query(sql, rse, params);
  }

  @Override
  public List<Map<String, Object>> findTopWithMap(String sql, int top, Object... params) {
    ResultSetExtractor<List<Map<String, Object>>> rse = new ListExtractor<Map<String, Object>>(getColumnMapRowMapper(), top);
    return query(sql, rse, params);
  }

  @Override
  public List<Map<String, Object>> findWithMap(String sql, Object... params) {
    try {
      convert(params);
      return jdbcTemplate.queryForList(sql, params);
    }
    catch (DataAccessException e) {
      throw translateException(e);
    }
  }

  @Override
  public <E> PageContainer<E> findWithPagination(String sql, int page, int rowsPerPage, Class<E> clazz, Object... params) {
    ResultSetExtractor<PageContainer<E>> rse = new PaginateExtractor<E>(new BaseBeanPropertyRowMapper<E>(clazz, columnMapper), page, rowsPerPage);
    return query(sql, rse, params);
  }

  @Override
  public PageContainer<Map<String, Object>> findWithPaginationMap(String sql, int page, int rowsPerPage, Object... params) {
    ResultSetExtractor<PageContainer<Map<String, Object>>> rse = new PaginateExtractor<Map<String, Object>>(getColumnMapRowMapper(), page, rowsPerPage);
    return query(sql, rse, params);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <E extends TableEntity> E findByKey(E entity) {
    
    List<Object> keyValues = new ArrayList<Object>();
    StringBuilder sql = new StringBuilder("SELECT * FROM ");
    sql.append(entity.getTableName()).append(" WHERE ");
    sql.append(StatementUtil.assembleKeyStatement(entity, keyValues));
//    logger.debug(sql.toString() + " params: " + keyValues.toString());
    return (E) this.findOne(sql.toString(), entity.getClass(), keyValues.toArray());
  }
  
  protected RowMapper<Map<String, Object>> getColumnMapRowMapper() {
    return new ColumnMapRowMapper();
  }
  
  private <E> E query(String sql, ResultSetExtractor<E> extractor, Object... params) {

    E obj = null;
    try {
      convertParams(params);
      obj = jdbcTemplate.query(sql, params, extractor);//getQueryRunner().query(getConnection(), sql, rsHandler, params);
    }
    catch (DataAccessException e) {
      throw translateException(e);
    }
    return obj;
  }

  protected RuntimeException translateException(Exception e) {
    return new DatabaseException(e);
  }

  protected Object[] convertParams(Object[] params) {

    if (params != null) {
      for (int i = 0; i < params.length; i++) {
        params[i] = convert(params[i]);
      }
    }
    return params;
  }

  protected Object convert(Object param) {

    return TableEntityUtil.convertToSqlObject(param.getClass(), param);
  }
}
