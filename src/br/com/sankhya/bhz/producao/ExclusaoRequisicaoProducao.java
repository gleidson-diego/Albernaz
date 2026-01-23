package br.com.sankhya.bhz.producao;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;

public class ExclusaoRequisicaoProducao implements EventoProgramavelJava {

    @Override public void beforeInsert(PersistenceEvent event) throws Exception {}
    @Override public void afterInsert(PersistenceEvent event) throws Exception {}
    @Override public void beforeUpdate(PersistenceEvent event) throws Exception {}
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void afterDelete(PersistenceEvent event) throws Exception {}
    @Override public void beforeCommit(TransactionContext tranCtx) throws Exception {}

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {

        DynamicVO cabVO = (DynamicVO) event.getVo();
        if (cabVO == null) return;

        BigDecimal nuNota = cabVO.asBigDecimalOrZero("NUNOTA");
        if (nuNota == null || nuNota.compareTo(BigDecimal.ZERO) == 0) return;

        EntityFacade facade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = facade.getJdbcWrapper();

        NativeSql ns = null;
        ResultSet rs = null;

        try {
            jdbc.openSession();

            ns = new NativeSql(jdbc);
            ns.appendSql(
                    "SELECT " +
                            "   CAB.CODTIPOPER, " +
                            "   CAB.AD_IDIPROC AS IDIPROC, " +
                            "   (SELECT PROC.IDIPROC FROM TPRIPROC PROC WHERE PROC.IDIPROC = CAB.AD_IDIPROC) AS NUMOP, " +
                            "   (SELECT COUNT(1) " +
                            "      FROM TPRAPO APO " +
                            "      JOIN TPRIATV IATV ON IATV.IDIATV = APO.IDIATV " +
                            "     WHERE IATV.IDIPROC = CAB.AD_IDIPROC " +
                            "       AND APO.SITUACAO = 'C' " +
                            "   ) AS QTD_APO_CONF " +
                            "FROM TGFCAB CAB " +
                            "WHERE CAB.NUNOTA = :NUNOTA"
            );

            ns.setNamedParameter("NUNOTA", nuNota);
            rs = ns.executeQuery();

            if (!rs.next()) return;

            BigDecimal codTipOper = rs.getBigDecimal("CODTIPOPER");
            BigDecimal idiproc    = rs.getBigDecimal("IDIPROC");
            BigDecimal qtdConf    = rs.getBigDecimal("QTD_APO_CONF");
            String numOp          = rs.getString("NUMOP");

            // Aplica somente para TOP 129 (mesma regra do evento de alteração)
            if (codTipOper == null || codTipOper.compareTo(new BigDecimal("129")) != 0) {
                return;
            }

            // Sem OP vinculada, não bloqueia
            if (idiproc == null || idiproc.compareTo(BigDecimal.ZERO) == 0) {
                return;
            }

            // Se existir apontamento confirmado, bloqueia exclusão
            if (qtdConf != null && qtdConf.compareTo(BigDecimal.ZERO) > 0) {
                String op = (numOp == null || numOp.trim().isEmpty())
                        ? String.valueOf(idiproc)
                        : numOp.trim();

                throw new Exception(
                        "Exclusão não permitida pois existe apontamento confirmado para a OP: " + op + "."
                );
            }

        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
            try { jdbc.closeSession(); } catch (Exception ignored) {}
        }
    }
}
