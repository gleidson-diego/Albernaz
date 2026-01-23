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

public class EventoBloqueiaAlteracaoRequisicaoOP implements EventoProgramavelJava {

    @Override public void beforeInsert(PersistenceEvent event) throws Exception {}
    @Override public void afterInsert(PersistenceEvent event) throws Exception {}
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void afterDelete(PersistenceEvent event) throws Exception {}
    @Override public void beforeCommit(TransactionContext tranCtx) throws Exception {}

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {

        DynamicVO iteVO = (DynamicVO) event.getVo();
        DynamicVO oldVO = (DynamicVO) event.getOldVO();

        // Só valida se a quantidade foi alterada
        if (oldVO == null) return;

        BigDecimal qtdNova = iteVO.asBigDecimalOrZero("QTDNEG");
        BigDecimal qtdAnt  = oldVO.asBigDecimalOrZero("QTDNEG");

        if (safeCompare(qtdNova, qtdAnt) == 0) return;

        BigDecimal nuNota = iteVO.asBigDecimalOrZero("NUNOTA");
        if (nuNota == null || nuNota.compareTo(BigDecimal.ZERO) == 0) return;

        validaApontamentoConfirmadoBloqueia(nuNota, "Edição não permitida pois o apontamento da OP: ");
    }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {

        // Evento na instância ItemNota (TGFITE): bloqueia exclusão do item
        DynamicVO iteVO = (DynamicVO) event.getVo();
        if (iteVO == null) return;

        BigDecimal nuNota = iteVO.asBigDecimalOrZero("NUNOTA");
        if (nuNota == null || nuNota.compareTo(BigDecimal.ZERO) == 0) return;

        // Opcional: pega sequência do item para detalhar mensagem
        BigDecimal sequencia = iteVO.asBigDecimalOrZero("SEQUENCIA");

        String detalheItem = (sequencia != null && sequencia.compareTo(BigDecimal.ZERO) > 0)
                ? " (Item seq. " + sequencia + ")"
                : "";

        validaApontamentoConfirmadoBloqueia(
                nuNota,
                "Exclusão não permitida" + detalheItem + " pois o apontamento da OP: "
        );
    }

    /**
     * Reuso da mesma regra do beforeUpdate:
     * - Aplica somente para TOP 129
     * - Exige CAB.AD_IDIPROC preenchido
     * - Se existir TPRAPO.SITUACAO = 'C' para a OP, bloqueia
     */
    private void validaApontamentoConfirmadoBloqueia(BigDecimal nuNota, String msgPrefix) throws Exception {

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

            // Aplica somente para TOP 129
            if (codTipOper == null || codTipOper.compareTo(new BigDecimal("129")) != 0) return;

            // Sem OP vinculada, não bloqueia
            if (idiproc == null || idiproc.compareTo(BigDecimal.ZERO) == 0) return;

            // Se existir apontamento confirmado, bloqueia
            if (qtdConf != null && qtdConf.compareTo(BigDecimal.ZERO) > 0) {
                String op = (numOp == null || numOp.trim().isEmpty())
                        ? String.valueOf(idiproc)
                        : numOp.trim();

                throw new Exception(msgPrefix + op + " foi confirmado!");
            }

        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
            try { jdbc.closeSession(); } catch (Exception ignored) {}
        }
    }

    private int safeCompare(BigDecimal a, BigDecimal b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        return a.compareTo(b);
    }
}
