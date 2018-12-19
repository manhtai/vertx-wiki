package com.manhtai.vertx.wiki.database;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.serviceproxy.ServiceBinder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class WikiDatabaseVerticle extends AbstractVerticle {
  public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
  public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
  public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
  public static final String CONFIG_WIKIDB_JDBC_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";

  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  public static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle.class);
  private JDBCClient jdbcClient;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    HashMap<SqlQuery, String> sqlQueries = loadSqlQuery();

    jdbcClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
      .put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
      .put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30)));

    WikiDatabaseService.create(jdbcClient, sqlQueries, ready -> {
      if(ready.succeeded()) {
        ServiceBinder binder = new ServiceBinder(vertx);
        binder.setAddress(CONFIG_WIKIDB_QUEUE).register(WikiDatabaseService.class, ready.result());
        startFuture.complete();
      } else {
        startFuture.fail(ready.cause());
      }
    });
  }

  private HashMap<SqlQuery, String> loadSqlQuery() throws IOException {
    String queriesFile = config().getString(CONFIG_WIKIDB_JDBC_SQL_QUERIES_RESOURCE_FILE);
    InputStream queriesInputStream;

    if (queriesFile != null) {
      queriesInputStream = new FileInputStream(queriesFile);
    } else {
      queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
    }

    Properties queriesPros = new Properties();
    queriesPros.load(queriesInputStream);
    queriesInputStream.close();

    HashMap<SqlQuery, String> sqlQueries = new HashMap<>();
    sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesPros.getProperty("create-pages-table"));
    sqlQueries.put(SqlQuery.ALL_PAGES, queriesPros.getProperty("all-pages"));
    sqlQueries.put(SqlQuery.GET_PAGE, queriesPros.getProperty("get-page"));
    sqlQueries.put(SqlQuery.CREATE_PAGE, queriesPros.getProperty("create-page"));
    sqlQueries.put(SqlQuery.SAVE_PAGE, queriesPros.getProperty("save-page"));
    sqlQueries.put(SqlQuery.DELETE_PAGE, queriesPros.getProperty("delete-page"));
    return sqlQueries;
  }
}
