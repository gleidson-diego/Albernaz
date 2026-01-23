package br.com.sankhya.bhz.objetosBanco;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class AcaoCriarObjetos implements AcaoRotinaJava {
    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        EntityFacade dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = dwfEntityFacade.getJdbcWrapper();
        jdbc.openSession();

        NativeSql sql = new NativeSql(jdbc);
        sql.loadSql(AcaoCriarObjetos.class, "sql/Func_BHZ_FC_BUSCA_KM.sql");
        sql.executeUpdate();

        NativeSql sql2 = new NativeSql(jdbc);
        sql2.loadSql(AcaoCriarObjetos.class, "sql/Func_BHZ_FC_BUSCA_VLR_CONSULTOR.sql");
        sql2.executeUpdate();

        NativeSql sql3 = new NativeSql(jdbc);
        sql3.loadSql(AcaoCriarObjetos.class, "sql/Func_BHZ_FC_HORAS_DEC.sql");
        sql3.executeUpdate();

        NativeSql sql4 = new NativeSql(jdbc);
        sql4.loadSql(AcaoCriarObjetos.class, "sql/Func_BHZ_FC_HORAS_STR.sql");
        sql4.executeUpdate();

        NativeSql sql5 = new NativeSql(jdbc);
        sql5.loadSql(AcaoCriarObjetos.class, "sql/View_BHZ_VW_PAINELGERENCIA.sql");
        sql5.executeUpdate();

        NativeSql sql6 = new NativeSql(jdbc);
        sql6.loadSql(AcaoCriarObjetos.class, "sql/View_BHZ_VW_PAINELGERENCIA_CON.sql");
        sql6.executeUpdate();

        NativeSql sql7 = new NativeSql(jdbc);
        sql7.loadSql(AcaoCriarObjetos.class, "sql/View_BHZ_VW_PAINELGERENCIA_USU.sql");
        sql7.executeUpdate();

        jdbc.closeSession();
    }
}
