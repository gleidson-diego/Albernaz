package br.com.sankhya.bhz.utils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/*
    autor: Guilherme Alves
    data: 10/11/2018
 */

public class AcessoBanco {
    private EntityFacade dwf;
    private JdbcWrapper jdbc;
    private NativeSql sql;
    private boolean aberto = false;


    public AcessoBanco() throws SQLException {
        this.openSession();
    }

    public void openSession() throws SQLException {
        if (!aberto) {
            dwf = EntityFacadeFactory.getDWFFacade();
            jdbc = dwf.getJdbcWrapper();
            jdbc.openSession();
            aberto = true;
        }

    }

    public void closeSession() throws SQLException {
        if (aberto)
            jdbc.closeSession();
        aberto = false;
    }

    public JdbcWrapper getJdbc() {
        return jdbc;
    }

    public NativeSql getNativeSql() {
        return new NativeSql(jdbc);
    }

    public boolean isOpen() {
        return aberto;
    }

    public ResultSet find(String consulta, Object... params) throws Exception {
        sql = new NativeSql(jdbc);
        sql.appendSql(consulta);
        for (Object param : params) {
            sql.addParameter(param);
        }
        return sql.executeQuery();
    }

    public ResultSet findOne(String consulta, Object... params) throws Exception {
        sql = new NativeSql(jdbc);
        sql.appendSql(consulta);
        for (Object param : params) {
            sql.addParameter(param);
        }
        ResultSet rs = sql.executeQuery();
        if (rs.next())
            return rs;
        return null;
    }

    public void update(String contexto, Object... params) throws Exception {
        sql = new NativeSql(jdbc);
        sql.appendSql(contexto);
        for (Object param : params) {
            sql.addParameter(param);
        }
        sql.executeUpdate();
    }

    public void insertGeneric(String tabela, HashMap<String, Object> chaveValores) throws Exception {
        StringBuilder insert = new StringBuilder();
        StringBuilder values = new StringBuilder();

        insert.append(" INSERT INTO ").append(tabela).append(" ( ");
        values.append(" ) ").append(" VALUES ").append(" ( ");

        int auxiliarVirgula = 0;
        for(java.util.Map.Entry<String, ?> map : chaveValores.entrySet()){
            insert.append(map.getKey());
            values.append("'").append(map.getValue()).append("'");
            auxiliarVirgula++;
            if(auxiliarVirgula < chaveValores.size() ){
                insert.append(", ");
                values.append(", ");
            }
        }

        values.append(" ) ");

        sql = new NativeSql(jdbc);
        sql.appendSql(insert.append(values).toString());
        sql.executeUpdate();
    }

    public void delete(String contexto, Object... params) throws Exception {
        sql = new NativeSql(jdbc);
        sql.appendSql(contexto);
        for (Object param : params) {
            sql.addParameter(param);
        }
        sql.executeUpdate();
    }

}
