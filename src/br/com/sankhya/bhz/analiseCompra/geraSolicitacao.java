/***
 * Created by: Fabio Barroso
 * Date: 28/03/2019
 */
package br.com.sankhya.bhz.analiseCompra;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.bhz.utils.ErroUtils;
import br.com.sankhya.bhz.utils.duplicarRegistro;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.BarramentoRegra;
import br.com.sankhya.modelcore.comercial.centrais.CACHelper;
import br.com.sankhya.modelcore.comercial.impostos.ImpostosHelpper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.MGECoreParameter;
import com.sankhya.util.TimeUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class geraSolicitacao implements AcaoRotinaJava {
    private final JapeWrapper varDAO = JapeFactory.dao("CompraVendavariosPedido");

    @Override
    public void doAction(ContextoAcao contextoAcao) throws Exception {

        JdbcWrapper jdbc = null;
        jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
        BigDecimal codUsuario = contextoAcao.getUsuarioLogado();

        JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
        JapeWrapper iteDAO = JapeFactory.dao("ItemNota");
        JapeWrapper proDAO = JapeFactory.dao("Produto");
        JapeWrapper parDAO = JapeFactory.dao("Parceiro");
        JapeWrapper cidDAO = JapeFactory.dao("Cidade");
        JapeWrapper usuDAO = JapeFactory.dao("Usuario");
        JapeWrapper ufsDAO = JapeFactory.dao("UnidadeFederativa");
        JapeWrapper topDAO = JapeFactory.dao("TipoOperacao");
        JapeWrapper cplDAO = JapeFactory.dao("ComplementoParc");
        JapeWrapper vendDAO = JapeFactory.dao("Vendedor");

        String msgRetorno = " ";

        DynamicVO usuVO = usuDAO.findOne("CODUSU = ?",codUsuario);
        DynamicVO vendVO = vendDAO.findOne("CODPARC = ? AND ATIVO = 'S'",usuVO.asBigDecimalOrZero("CODPARC"));

        String agrupa = contextoAcao.getParam("AGRUPA").toString();

        Object codTipOperObj = contextoAcao.getParam("CODTIPOPER");
        if (codTipOperObj == null) {
            throw new Exception("Parâmetro de TOP não foi informado!");
        }
        BigDecimal codTipOper = new BigDecimal(codTipOperObj.toString());


        Object codParcParamObj = contextoAcao.getParam("CODPARC");
        BigDecimal codParcParam = null;

        if (codParcParamObj != null && !codParcParamObj.toString().trim().isEmpty()) {
            codParcParam = new BigDecimal(codParcParamObj.toString());
        }


        Object codLocalObj = contextoAcao.getParam("CODLOCAL");
        if (codLocalObj == null) {
            throw new Exception("Parâmetro de Local não foi informado!");
        }
        BigDecimal codLocal = new BigDecimal(codLocalObj.toString());


        DynamicVO topVO = topDAO.findOne("CODTIPOPER = ? AND DHALTER = (SELECT MAX(DHALTER) FROM TGFTOP TP WHERE TP.CODTIPOPER = TGFTOP.CODTIPOPER)",codTipOper);

        String operMoeda = String.valueOf("N");

        if (topVO.asString("OPERCOMMOEDA") != null  && topVO.asString("OPERCOMMOEDA").equals("S")) {
            operMoeda= String.valueOf("S");
        }

        if(null!=topVO && !"O".equals(topVO.asString("TIPMOV")) && !"J".equals(topVO.asString("TIPMOV"))){
            ErroUtils.disparaErro("Apenas top's de Pedido de Requisição e Pedido de Compras podem ser usadas aqui!!!");
        }

        boolean validaIns = false;
        Registro[] linhas = contextoAcao.getLinhas();
        DynamicVO cabVO = null;
        BigDecimal codEmp = BigDecimal.ZERO;
        BigDecimal codParc = BigDecimal.ONE;
        String uf = "MG";
        Collection<BigDecimal> nuNotas = new ArrayList();
        String obs = "GERADO PELA ANALISE DE COMPRA";
        Timestamp dtPrevEnt = null;

        for (Registro linha : linhas) {

            Object codParcObj = linha.getCampo("CODPARC");
            if (codParcObj == null) {
                throw new Exception("Parceiro está nulo!");
            }
            BigDecimal codParcLinha = new BigDecimal(codParcObj.toString());


            if (codParcParam != null) {
                codParc = codParcParam;
            } else {
                codParc = codParcLinha;
            }

            codEmp = new BigDecimal(linha.getCampo("CODEMP").toString());
            dtPrevEnt =  (Timestamp) linha.getCampo("DTINI");
            DynamicVO parVO = parDAO.findOne("CODPARC = ?",codParc);
            DynamicVO cplVO = cplDAO.findOne("CODPARC = ?",codParc);
            String nomeParceiro = parVO.asBigDecimalOrZero("CODPARC").toString().concat(" - ").concat(parVO.asString("NOMEPARC"));
            if(null!=parVO && parVO.asBigDecimal("CODPARC").compareTo(BigDecimal.ZERO)!=0){
                DynamicVO cidVO = cidDAO.findOne("CODCID = ?",parVO.asBigDecimalOrZero("CODCID"));
                if(null!=cidVO){
                    DynamicVO ufsVO = ufsDAO.findOne("CODUF = ?",cidVO.asBigDecimalOrZero("UF"));
                    if(null!=ufsVO){
                        uf = ufsVO.asString("UF");
                    }
                }
            }

            BigDecimal modeloNota = (BigDecimal) MGECoreParameter.getParameter("BHZ_MODSOLICIT");

            if("EX".equals(uf)) {
                modeloNota = (BigDecimal) MGECoreParameter.getParameter("BHZ_MODSOLICITI");
            }

            if (modeloNota == null) {
                ErroUtils.disparaErro("Parâmetro do modelo de solicitação não configurado. Verifique o parâmetro "
                        + ("EX".equals(uf) ? "BHZ_MODSOLICITI" : "BHZ_MODSOLICIT") + ".");
            }

            DynamicVO modeloNotaVO = cabDAO.findOne("NUNOTA = ?", modeloNota);

            if (modeloNotaVO == null) {
                ErroUtils.disparaErro("Nota modelo de solicitação não encontrada. Verifique o parâmetro "
                        + ("EX".equals(uf) ? "BHZ_MODSOLICITI" : "BHZ_MODSOLICIT") + ": " + modeloNota + ".");
            }

//            System.out.println("codEmp=" + codEmp);
//            System.out.println("codTipOper=" + codTipOper);
//            System.out.println("codParc=" + codParc);
//            System.out.println("codUsuario=" + codUsuario);
//            System.out.println("obs=" + obs);


            BigDecimal qtdNeg = new BigDecimal (linha.getCampo("SUGERIDO").toString());

            if (qtdNeg.compareTo(BigDecimal.ZERO) <= 0) {
                continue;
            }

            cabVO = cabDAO.findOne("CODEMP = ? AND CODTIPOPER = ? AND CODPARC = ? AND STATUSNOTA != 'L' AND TRUNC(DTNEG) = TRUNC(?) AND CODUSU = ? AND CAST(OBSERVACAO AS VARCHAR(1000)) = ?"
                    ,codEmp,codTipOper,codParc,TimeUtils.getNow(), codUsuario, obs);

            if (cabVO == null)  {
                Map<String, Object> nota = new HashMap<>();

                nota.put("STATUSNOTA", "A");
                nota.put("NUMNOTA", BigDecimal.ZERO);
                nota.put("DTNEG", TimeUtils.getNow());
                nota.put("CODTIPOPER",codTipOper);
                nota.put("TIPMOV",topVO.asString("TIPMOV"));
                nota.put("DHTIPOPER", topVO.asTimestamp("DHALTER"));
                nota.put("DTFATUR", TimeUtils.getNow());
                nota.put("CODPARC", codParc);
                nota.put("DTENTSAI", TimeUtils.getNow());
                nota.put("CODEMP", codEmp);
                nota.put("CODUSU", codUsuario);
                nota.put("DTPREVENT", dtPrevEnt);
                if(BigDecimal.ZERO.compareTo(usuVO.asBigDecimalOrZero("CODVEND")) > 0){
                    nota.put("CODVEND",usuVO.asBigDecimalOrZero("CODVEND"));
                } else if (vendVO != null) {
                    nota.put("CODVEND", vendVO.asBigDecimalOrZero("CODVEND"));
                }

                nota.put("OBSERVACAO", obs);



                if(cplVO != null) {
                    if (BigDecimal.ZERO.compareTo(cplVO.asBigDecimalOrZero("SUGTIPNEGENTR")) != 0) {
                        nota.put("CODTIPVENDA", cplVO.asBigDecimalOrZero("SUGTIPNEGENTR"));
                        nota.put("DHTIPVENDA", duplicarRegistro.getDataMaxTipoNeg(cplVO.asBigDecimalOrZero("SUGTIPNEGENTR")));
                    }
                }
                nota.put("DTMOV", TimeUtils.getNow());

                cabVO = duplicarRegistro.duplicaRegistroVO(modeloNotaVO, DynamicEntityNames.CABECALHO_NOTA, nota);
            }

            if (cabVO.asTimestamp("DTPREVENT") != null && dtPrevEnt.before(cabVO.asTimestamp("DTPREVENT"))) {
                cabDAO.prepareToUpdateByPK(cabVO.asBigDecimalOrZero("NUNOTA"))
                        .set("DTPREVENT", dtPrevEnt)
                        .update();
            }


            /*BUSCA CUSTO*/
            NativeSql sql = new NativeSql(jdbc);
            sql.loadSql(geraSolicitacao.class, "sql/buscaCustoPreco.sql");
            sql.setNamedParameter("CODPROD",linha.getCampo("CODPRODMP"));
            sql.setNamedParameter("CODTAB",parVO.asBigDecimalOrZero("CODTAB"));
            ResultSet resultSet = sql.executeQuery();
            BigDecimal custo = BigDecimal.ZERO;

            if (resultSet.next()){
                custo = resultSet.getBigDecimal("CUSREP");
            }

            DynamicVO iteVO = null;
            if ("S".equals(agrupa)){
                iteVO = iteDAO.findOne("NUNOTA = ? AND CODPROD = ? AND CONTROLE = ? "
                        , cabVO.asBigDecimal("NUNOTA"), linha.getCampo("CODPRODMP"), linha.getCampo("CONTROLEMP"));
                if (iteVO!=null) {
                    qtdNeg = qtdNeg.add(iteVO.asBigDecimalOrZero("QTDNEG"));
                }
            } else {
                iteVO = iteDAO.findOne("NUNOTA = ? AND CODPROD = ? AND CONTROLE = ? AND DTINICIO = ?"
                        , cabVO.asBigDecimal("NUNOTA"), linha.getCampo("CODPRODMP"), linha.getCampo("CONTROLEMP"), linha.getCampo("DTINI"));
            }

            if (iteVO == null && qtdNeg.compareTo(BigDecimal.ZERO)>0) {
                DynamicVO proVO = proDAO.findOne("CODPROD = ?",linha.getCampo("CODPRODMP"));
                String tipo = String.valueOf(linha.getCampo("TIPO"));

                DynamicVO novoItem = iteDAO.create()
                        .set("CODPROD", linha.getCampo("CODPRODMP"))
                        .set("CONTROLE", linha.getCampo("CONTROLEMP"))
                        .set("QTDNEG", qtdNeg)
                        .set("CODEMP", cabVO.asBigDecimal("CODEMP"))
                        .set("VLRUNIT", custo)
                        .set("VLRUNITMOE", operMoeda.equals("S") ? custo : BigDecimal.ZERO)
                        .set("VLRUNITLIQMOE", operMoeda.equals("S") ? custo : BigDecimal.ZERO)
                        .set("VLRTOT",qtdNeg.multiply(custo))
                        .set("VLRTOTMOE",operMoeda.equals("S") ? qtdNeg.multiply(custo) : BigDecimal.ZERO)
                        .set("NUNOTA", cabVO.asBigDecimal("NUNOTA"))
                        .set("USOPROD", proVO.asString("USOPROD"))
                        .set("CODLOCALORIG", codLocal)
                        .set("ATUALESTOQUE", BigDecimal.ZERO)
                        .set("CODVOL", proVO.asString("CODVOL"))
                        .set("DTINICIO", linha.getCampo("DTINI"))
                        .save();

                  if ("S".equals(tipo)
                        && linha.getCampo("NUMPS") != null
                        && linha.getCampo("SEQIMRP") != null){

                    gerarVarSeNecessario(
                            cabVO.asBigDecimal("NUNOTA"),                  // NUNOTA destino (pedido)
                            novoItem.asBigDecimal("SEQUENCIA"),            // SEQUENCIA destino
                            linha.getCampo("NUMPS") != null
                                    ? new BigDecimal(linha.getCampo("NUMPS").toString())
                                    : null,                                  // NUNOTA origem
                            linha.getCampo("SEQIMRP") != null
                                    ? new BigDecimal(linha.getCampo("SEQIMRP").toString())
                                    : null,                                  // SEQUENCIA origem
                            new BigDecimal (linha.getCampo("SUGERIDO").toString())   // QTDATENDIDA
                    );
                 }

                validaIns = true;
                adicionarNuNota(nuNotas, cabVO.asBigDecimal("NUNOTA"));
            } else {
                if (iteVO != null && iteVO.asBigDecimal("QTDNEG").compareTo(qtdNeg)<0){
                    iteDAO.prepareToUpdate(iteVO)
                            .set("QTDNEG",qtdNeg)
                            .set("VLRTOT",qtdNeg.multiply(custo))
                            .update();
                    validaIns = true;
                    adicionarNuNota(nuNotas, cabVO.asBigDecimal("NUNOTA"));
                }
            }
        }

        if (validaIns) {
            msgRetorno = gerarMensagemRetorno(nuNotas, topVO.asString("TIPMOV"));
        } else {
            ErroUtils.disparaErro("Nenhum item selecionado tem qtd. sugerida!!!");
        }

        confirmaSolicitacoes(nuNotas);
        contextoAcao.setMensagemRetorno(msgRetorno);
    }

    public void confirmaSolicitacoes(Collection<BigDecimal> nuNotas) throws Exception{
        for(BigDecimal nuNota : nuNotas){
            ImpostosHelpper impHelper = new ImpostosHelpper();
            BarramentoRegra bRegras = BarramentoRegra.build(CACHelper.class, "regrasConfirmacaoCAC.xml", AuthenticationInfo.getCurrent());
            impHelper.setForcarRecalculo(true);
            impHelper.calcularImpostos(nuNota);
            impHelper.totalizarNota(nuNota);
            impHelper.salvarNota();
//            ConfirmacaoNotaHelper.confirmarNota(nuNota, bRegras, false);
        }
    }

    private void adicionarNuNota(Collection<BigDecimal> nuNotas, BigDecimal nuNota) {
        if (nuNota != null && !nuNotas.contains(nuNota)) {
            nuNotas.add(nuNota);
        }
    }

    private String gerarMensagemRetorno(Collection<BigDecimal> nuNotas, String tipMov) {
        StringBuilder mensagem = new StringBuilder();

        mensagem.append("<div style=\"text-align: center; padding-top: 7px;\">");
        mensagem.append("Pedidos inseridos com sucesso.<br><br>");

        if (nuNotas.size() == 1) {
            BigDecimal nuNota = nuNotas.iterator().next();
            mensagem.append("Numero unico gerado: ");
            mensagem.append(gerarLinkCentralNotas(nuNota, tipMov));
        } else {
            mensagem.append("Numeros unicos gerados:<br>");

            for (BigDecimal nuNota : nuNotas) {
                mensagem.append(gerarLinkCentralNotas(nuNota, tipMov));
                mensagem.append("<br>");
            }
        }

        mensagem.append("</div>");

        return mensagem.toString();
    }

    private String gerarLinkCentralNotas(BigDecimal nuNota, String tipMov) {
        String urlCentral = gerarUrlCentralNotas(nuNota, tipMov);
        return "<a id=\"alink\" href=\"" + urlCentral + "\" target=\"_top\" style=\"font-weight: bold;\">" + nuNota + "</a>";
    }

    private String gerarUrlCentralNotas(BigDecimal nuNota, String tipMov) {
        String classeCentral = toBase64("br.com.sankhya.com.mov.CentralNotas");
        String parametros = "{\"NUNOTA\":" + nuNota
                + ", \"TIPMOV\":\"" + tipMov + "\""
                + ", \"ehPedidoW\":false"
                + ", \"forceNewHash\":" + System.currentTimeMillis()
                + "}";

        return "/mge/system.jsp#app/" + classeCentral + "/" + toBase64(parametros) + "&pk-refresh=" + System.currentTimeMillis();
    }

    private String toBase64(String texto) {
        return Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
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

