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

public class BloqueiaDevolucaoRequisicaoOP implements EventoProgramavelJava {

    private static final BigDecimal TOP_REQUISICAO_OP = new BigDecimal("129");

    @Override public void beforeUpdate(PersistenceEvent event) throws Exception {}
    @Override public void afterInsert(PersistenceEvent event) throws Exception {}
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void beforeDelete(PersistenceEvent event) throws Exception {}
    @Override public void afterDelete(PersistenceEvent event) throws Exception {}
    @Override public void beforeCommit(TransactionContext tranCtx) throws Exception {}

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {

        DynamicVO varVO = (DynamicVO) event.getVo();

        BigDecimal nuNota = varVO.asBigDecimalOrZero("NUNOTA");
        BigDecimal nuNotaOrig = varVO.asBigDecimalOrZero("NUNOTAORIG");

        // Precisa de NUNOTA (nota atual) e NUNOTAORIG (origem)
        if (nuNota == null || nuNota.compareTo(BigDecimal.ZERO) == 0) return;
        if (nuNotaOrig == null || nuNotaOrig.compareTo(BigDecimal.ZERO) == 0) return;

        EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = dwfFacade.getJdbcWrapper();

        try {
            jdbc.openSession();

            // 1) Só atua se a nota atual for TIPMOV = 'L'
            if (!notaEhTipMov(jdbc, nuNota, "L")) {
                return;
            }

            // 2) Só atua se a nota origem (NUNOTAORIG) for TOP 129
            if (!notaEhTop(jdbc, nuNotaOrig, TOP_REQUISICAO_OP)) {
                return;
            }

            // 3) Recupera a OP (IDIPROC) vinculada à requisição (TOP 129)
            BigDecimal idiproc = buscaIdiprocDaRequisicaoOP(jdbc, nuNotaOrig);
            if (idiproc == null || idiproc.compareTo(BigDecimal.ZERO) == 0) {
                // sem OP vinculada, não aplica a regra do apontamento confirmado
                return;
            }

            // 4) Só bloqueia se existir apontamento confirmado (TPRAPO.SITUACAO = 'C') para a OP
            if (existeApontamentoConfirmadoDaOP(jdbc, idiproc)) {
                throw new Exception("Edição não permitida pois o apontamento  da OP: " + idiproc + " foi confirmado!");
            }

        } finally {
            try { jdbc.closeSession(); } catch (Exception ignored) {}
        }
    }

    private boolean notaEhTipMov(JdbcWrapper jdbc, BigDecimal nuNota, String tipMovEsperado) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ResultSet rs = null;
        try {
            ns.appendSql("SELECT CAB.TIPMOV FROM TGFCAB CAB WHERE CAB.NUNOTA = :NUNOTA");
            ns.setNamedParameter("NUNOTA", nuNota);
            rs = ns.executeQuery();
            if (!rs.next()) return false;
            String tipMov = rs.getString("TIPMOV");
            return tipMovEsperado.equalsIgnoreCase(tipMov);
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
        }
    }

    private boolean notaEhTop(JdbcWrapper jdbc, BigDecimal nuNota, BigDecimal codTipOperEsperado) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ResultSet rs = null;
        try {
            ns.appendSql("SELECT CAB.CODTIPOPER FROM TGFCAB CAB WHERE CAB.NUNOTA = :NUNOTA");
            ns.setNamedParameter("NUNOTA", nuNota);
            rs = ns.executeQuery();
            if (!rs.next()) return false;
            BigDecimal codTipOper = rs.getBigDecimal("CODTIPOPER");
            return codTipOper != null && codTipOper.compareTo(codTipOperEsperado) == 0;
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Busca o IDIPROC vinculado à requisição OP (TOP 129).
     * Você informou que a ligação é via TGFCAB.AD_IDIPROC.
     */
    private BigDecimal buscaIdiprocDaRequisicaoOP(JdbcWrapper jdbc, BigDecimal nuNotaRequisicao) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ResultSet rs = null;
        try {
            ns.appendSql("SELECT CAB.AD_IDIPROC AS IDIPROC FROM TGFCAB CAB WHERE CAB.NUNOTA = :NUNOTA");
            ns.setNamedParameter("NUNOTA", nuNotaRequisicao);
            rs = ns.executeQuery();
            if (!rs.next()) return null;
            return rs.getBigDecimal("IDIPROC");
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Verifica apontamento confirmado:
     * TPRAPO (CabecalhoApontamento) -> TPRIATV (InstanciaAtividade) -> IDIPROC
     * Condição: TPRAPO.SITUACAO = 'C'
     */
    private boolean existeApontamentoConfirmadoDaOP(JdbcWrapper jdbc, BigDecimal idiproc) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ResultSet rs = null;
        try {
            ns.appendSql(
                    "SELECT 1 " +
                            "  FROM TPRAPO APO " +
                            "  JOIN TPRIATV IATV ON IATV.IDIATV = APO.IDIATV " +
                            " WHERE IATV.IDIPROC = :IDIPROC " +
                            "   AND APO.SITUACAO = 'C' " +
                            "   AND ROWNUM = 1"
            );
            ns.setNamedParameter("IDIPROC", idiproc);
            rs = ns.executeQuery();
            return rs.next();
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
        }
    }
}
