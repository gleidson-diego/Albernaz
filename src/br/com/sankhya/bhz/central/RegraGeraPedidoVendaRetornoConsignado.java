package br.com.sankhya.bhz.central;

import br.com.sankhya.bhz.utils.Utilitarios;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.modelcore.comercial.ContextoRegra;
import br.com.sankhya.modelcore.comercial.Regra;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.sankhya.util.TimeUtils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class RegraGeraPedidoVendaRetornoConsignado implements Regra {

    private static final BigDecimal TOP_PEDIDO_VENDA = BigDecimal.valueOf(55);
    private static final BigDecimal TOP_RETORNO = BigDecimal.valueOf(102);

    private final JapeWrapper cabDAO = JapeFactory.dao(DynamicEntityNames.CABECALHO_NOTA);
    private final JapeWrapper iteDAO = JapeFactory.dao(DynamicEntityNames.ITEM_NOTA);
    private final JapeWrapper topDAO = JapeFactory.dao(DynamicEntityNames.TIPO_OPERACAO);
    private final JapeWrapper varDAO = JapeFactory.dao("CompraVendavariosPedido");

    @Override
    public void beforeInsert(ContextoRegra ctx) throws Exception {
    }

    @Override
    public void beforeUpdate(ContextoRegra ctx) throws Exception {
    }

    @Override
    public void beforeDelete(ContextoRegra ctx) throws Exception {
    }

    @Override
    public void afterInsert(ContextoRegra ctx) throws Exception {
    }

    @Override
    public void afterUpdate(ContextoRegra ctx) throws Exception {

        boolean tgfCab = "CabecalhoNota".equals(ctx.getPrePersistEntityState().getDao().getEntityName());
        if (!tgfCab) {
            return;
        }

        boolean confirmando = JapeSession.getPropertyAsBoolean("CabecalhoNota.confirmando.nota", Boolean.FALSE);
        if (!confirmando) {
            return;
        }

        DynamicVO cabVO = ctx.getPrePersistEntityState().getNewVO();
        if (cabVO == null) {
            return;
        }

        BigDecimal nuNotaRetorno = cabVO.asBigDecimalOrZero("NUNOTA");
        BigDecimal codTipOperRetorno = cabVO.asBigDecimalOrZero("CODTIPOPER");
        BigDecimal codParc = cabVO.asBigDecimalOrZero("CODPARC");

        if (nuNotaRetorno == null || nuNotaRetorno.compareTo(BigDecimal.ZERO) == 0) {
            return;
        }

        // Mantém a regra restrita ao retorno
        if (codTipOperRetorno == null || codTipOperRetorno.compareTo(TOP_RETORNO) != 0) {
            return;
        }

        // Novo gatilho: existência de item com AD_QTDDEVOL > 0
        if (!existeItemParaGerarPedido(nuNotaRetorno)) {
            return;
        }

        DynamicVO cabPedidoVO = buscarPedidoAbertoMesmoParceiro(codParc);

        BigDecimal nuNotaPedido;
        if (cabPedidoVO != null) {
            nuNotaPedido = cabPedidoVO.asBigDecimalOrZero("NUNOTA");
        } else {
            nuNotaPedido = gerarCabecalhoPedido(cabVO);
        }

        inserirItensPedido(nuNotaRetorno, nuNotaPedido);

        Utilitarios.totalizar(nuNotaPedido);
    }

    @Override
    public void afterDelete(ContextoRegra ctx) throws Exception {
    }

    private boolean existeItemParaGerarPedido(BigDecimal nuNotaRetorno) throws Exception {
        EntityFacade facade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = facade.getJdbcWrapper();
        NativeSql sql = new NativeSql(jdbc);
        ResultSet rs = null;

        try {
            sql.appendSql(
                    "SELECT COUNT(1) AS QTD " +
                            "  FROM TGFITE ITE " +
                            " WHERE ITE.NUNOTA = :NUNOTA " +
                            "   AND NVL(ITE.AD_QTDDEVOL, 0) > 0 "
            );

            sql.setNamedParameter("NUNOTA", nuNotaRetorno);

            rs = sql.executeQuery();
            if (rs.next()) {
                BigDecimal qtd = rs.getBigDecimal("QTD");
                return qtd != null && qtd.compareTo(BigDecimal.ZERO) > 0;
            }

            return false;
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    private DynamicVO buscarPedidoAbertoMesmoParceiro(BigDecimal codParc) throws Exception {
        return cabDAO.findOne(
                "NUNOTA = ( " +
                        "    SELECT MAX(C.NUNOTA) " +
                        "    FROM TGFCAB C " +
                        "    WHERE C.CODPARC = ? " +
                        "      AND C.CODTIPOPER = ? " +
                        "      AND C.TIPMOV = 'P' " +
                        "      AND NVL(C.STATUSNOTA, 'A') <> 'L' " +
                        ")",
                codParc, TOP_PEDIDO_VENDA
        );
    }

    private BigDecimal gerarCabecalhoPedido(DynamicVO cabOrigVO) throws Exception {

        DynamicVO topPedidoVO = topDAO.findOne(
                "CODTIPOPER = ? AND DHALTER = (SELECT MAX(T.DHALTER) FROM TGFTOP T WHERE T.CODTIPOPER = TGFTOP.CODTIPOPER)",
                TOP_PEDIDO_VENDA
        );

        if (topPedidoVO == null) {
            throw new Exception("Não foi possível localizar a TOP 55.");
        }

        BigDecimal codParc = cabOrigVO.asBigDecimalOrZero("CODPARC");
        BigDecimal codEmp = cabOrigVO.asBigDecimalOrZero("CODEMP");
        BigDecimal codTipVenda = cabOrigVO.asBigDecimalOrZero("CODTIPVENDA");
        Timestamp dhTipVenda = cabOrigVO.asTimestamp("DHTIPVENDA");

        Map<String, Object> alteracoes = new HashMap<>();
        alteracoes.put("CODPARC", codParc);
        alteracoes.put("CODEMP", codEmp);
        alteracoes.put("CODEMPNEGOC", codEmp);
        alteracoes.put("DTNEG", TimeUtils.getNow());
        alteracoes.put("DTENTSAI", TimeUtils.getNow());
        alteracoes.put("DTMOV", TimeUtils.getNow());
        alteracoes.put("HRENTSAI", TimeUtils.getNow());
        alteracoes.put("CODTIPOPER", topPedidoVO.asBigDecimalOrZero("CODTIPOPER"));
        alteracoes.put("DHTIPOPER", topPedidoVO.asTimestamp("DHALTER"));
        alteracoes.put("CODTIPVENDA", codTipVenda);
        alteracoes.put("DHTIPVENDA", dhTipVenda);
        alteracoes.put("STATUSNOTA", "A");
        alteracoes.put("OBSERVACAO", "Pedido gerado automaticamente a partir do retorno consignado NUNOTA " +
                cabOrigVO.asBigDecimalOrZero("NUNOTA"));

        DynamicVO cabPedidoVO = Utilitarios.duplicaRegistroVO(cabOrigVO, "CabecalhoNota", alteracoes);

        return cabPedidoVO.asBigDecimalOrZero("NUNOTA");
    }

    private void inserirItensPedido(BigDecimal nuNotaRetorno, BigDecimal nuNotaPedido) throws Exception {
        EntityFacade facade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = facade.getJdbcWrapper();
        NativeSql sql = new NativeSql(jdbc);
        ResultSet rs = null;

        DynamicVO cabPedidoVO = cabDAO.findByPK(nuNotaPedido);
        BigDecimal codEmp = cabPedidoVO.asBigDecimalOrZero("CODEMP");

        try {
            sql.loadSql(RegraGeraPedidoVendaRetornoConsignado.class, "sql/consultaItens.sql");
            sql.setNamedParameter("NUNOTA", nuNotaRetorno);
            rs = sql.executeQuery();

            while (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal("CODPROD");
                BigDecimal vlrUnit = rs.getBigDecimal("VLRUNIT");
                BigDecimal qtdNeg = rs.getBigDecimal("QTDNEG");
                String codVol = rs.getString("CODVOL");
                String controle = rs.getString("CONTROLE");
                BigDecimal codLocalOrig = rs.getBigDecimal("CODLOCALORIG");
                BigDecimal sequenciaOrig = rs.getBigDecimal("SEQUENCIARET");

                if (qtdNeg == null || qtdNeg.compareTo(BigDecimal.ZERO) <= 0) {
                    continue;
                }

                DynamicVO itemExistente = iteDAO.findOne(
                        "NUNOTA = ? AND CODPROD = ? AND NVL(CONTROLE, ' ') = NVL(?, ' ')",
                        nuNotaPedido, codProd, controle
                );

                BigDecimal sequenciaPedido;

                if (itemExistente != null) {
                    BigDecimal qtdAtual = itemExistente.asBigDecimalOrZero("QTDNEG");
                    BigDecimal novaQtd = qtdAtual.add(qtdNeg);
                    BigDecimal novoTotal = novaQtd.multiply(vlrUnit);

                    iteDAO.prepareToUpdate(itemExistente)
                            .set("QTDNEG", novaQtd)
                            .set("VLRUNIT", vlrUnit)
                            .set("VLRTOT", novoTotal)
                            .update();

                    sequenciaPedido = itemExistente.asBigDecimalOrZero("SEQUENCIA");
                } else {
                    FluidCreateVO creITE = iteDAO.create();
                    creITE.set("NUNOTA", nuNotaPedido);
                    creITE.set("CODEMP", codEmp);
                    creITE.set("CODPROD", codProd);
                    creITE.set("CODVOL", codVol);
                    creITE.set("QTDNEG", qtdNeg);
                    creITE.set("CONTROLE", controle);
                    creITE.set("CODLOCALORIG", codLocalOrig);
                    creITE.set("ATUALESTOQUE", BigDecimal.ZERO);
                    creITE.set("RESERVA", "N");
                    creITE.set("ATUALESTTERC", "N");
                    creITE.set("TERCEIROS", "N");
                    creITE.set("VLRUNIT", vlrUnit);
                    creITE.set("VLRTOT", qtdNeg.multiply(vlrUnit));

                    DynamicVO itemCriado = creITE.save();
                    sequenciaPedido = itemCriado.asBigDecimalOrZero("SEQUENCIA");
                }

                gerarVarSeNecessario(
                        nuNotaPedido,
                        sequenciaPedido,
                        nuNotaRetorno,
                        sequenciaOrig,
                        qtdNeg
                );
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    private void gerarVarSeNecessario(BigDecimal nuNotaPedido,
                                      BigDecimal sequenciaPedido,
                                      BigDecimal nuNotaRetorno,
                                      BigDecimal sequenciaOrig,
                                      BigDecimal qtdAtendida) throws Exception {

        DynamicVO varExistente = varDAO.findOne(
                "NUNOTA = ? AND SEQUENCIA = ? AND NUNOTAORIG = ? AND SEQUENCIAORIG = ?",
                nuNotaPedido, sequenciaPedido, nuNotaRetorno, sequenciaOrig
        );

        if (varExistente != null) {
            BigDecimal qtdAtual = varExistente.asBigDecimalOrZero("QTDATENDIDA");

            varDAO.prepareToUpdate(varExistente)
                    .set("QTDATENDIDA", qtdAtual.add(qtdAtendida))
                    .set("STATUSNOTA", "A")
                    .update();
            return;
        }

        FluidCreateVO varVO = varDAO.create();
        varVO.set("NUNOTA", nuNotaPedido);
        varVO.set("SEQUENCIA", sequenciaPedido);
        varVO.set("NUNOTAORIG", nuNotaRetorno);
        varVO.set("SEQUENCIAORIG", sequenciaOrig);
        varVO.set("QTDATENDIDA", qtdAtendida);
        varVO.set("STATUSNOTA", "A");
        varVO.set("CUSATEND", null);
        varVO.set("FIXACAO", null);
        varVO.set("NROATOCONCDRAW", null);
        varVO.set("NROMEMORANDO", null);
        varVO.set("NROREGEXPORT", null);
        varVO.set("ORDEMPROD", null);
        varVO.save();
    }
}