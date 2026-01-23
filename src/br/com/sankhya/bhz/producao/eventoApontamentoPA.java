package br.com.sankhya.bhz.producao;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;

public class eventoApontamentoPA implements EventoProgramavelJava {
    public eventoApontamentoPA() {
    }

    public void beforeInsert(PersistenceEvent event) throws Exception {
    }

    public void beforeUpdate(PersistenceEvent event) throws Exception {
    }

    public void beforeDelete(PersistenceEvent event) throws Exception {
    }

    public void afterInsert(PersistenceEvent event) throws Exception {
        JapeWrapper ampDAO = JapeFactory.dao("ApontamentoMateriais");
        JapeWrapper apoDAO = JapeFactory.dao("CabecalhoApontamento");
        JapeWrapper iatvDAO = JapeFactory.dao("InstanciaAtividade");
        JapeWrapper atvDAO = JapeFactory.dao("Atividade");
        JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
        JapeWrapper iteDAO = JapeFactory.dao("ItemNota");
        DynamicVO apaVO = (DynamicVO)event.getVo();
        DynamicVO apoVO = apoDAO.findOne("NUAPO = ?", apaVO.asBigDecimalOrZero("NUAPO"));
        DynamicVO iatvVO = iatvDAO.findOne("IDIATV = ?", apoVO.asBigDecimalOrZero("IDIATV"));
        DynamicVO atvVO = atvDAO.findOne("IDEFX = ?", iatvVO.asBigDecimalOrZero("IDEFX"));
        if (null != atvVO && !atvVO.asString("APONTAMP").equals("N")) {

            EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
            JdbcWrapper jdbc = dwfFacade.getJdbcWrapper();
            jdbc.openSession();
            NativeSql sql = new NativeSql(jdbc);
            sql.loadSql(eventoApontamentoPA.class, "sql/buscaMP.sql");
            sql.setNamedParameter("IDIPROC", iatvVO.asBigDecimal("IDIPROC"));
            ResultSet resultSet = sql.executeQuery();

            while(resultSet.next()) {
                DynamicVO ampVO = ampDAO.findOne("CODPRODMP = ? AND CONTROLEMP = ? AND NUAPO = ? AND SEQAPA = ?"
                        ,resultSet.getBigDecimal("CODPROD")
                        ,resultSet.getString("CONTROLE")
                        ,apaVO.asBigDecimal("NUAPO")
                        ,apaVO.asBigDecimal("SEQAPA"));
                if(null==ampVO) {

                    ampDAO.create()
                            .set("NUAPO", apaVO.asBigDecimal("NUAPO"))
                            .set("SEQAPA", apaVO.asBigDecimal("SEQAPA"))
                            .set("CODPRODMP", resultSet.getBigDecimal("CODPROD"))
                            .set("QTD", resultSet.getBigDecimal("QTDNEG"))
                            .set("CODVOL", resultSet.getString("CODVOL"))
                            .set("CONTROLEMP", resultSet.getString("CONTROLE"))
                            .set("VINCULOSERIEPA", "N")
                            .save();
                }
            }
        }
    }

    public void afterUpdate(PersistenceEvent event) throws Exception {
    }

    public void afterDelete(PersistenceEvent event) throws Exception {
    }

    public void beforeCommit(TransactionContext tranCtx) throws Exception {
    }
}
