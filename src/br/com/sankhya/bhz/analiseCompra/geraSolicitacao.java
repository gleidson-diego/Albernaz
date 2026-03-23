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
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.BarramentoRegra;
import br.com.sankhya.modelcore.comercial.centrais.CACHelper;
import br.com.sankhya.modelcore.comercial.impostos.ImpostosHelpper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.MGECoreParameter;
import com.sankhya.util.TimeUtils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class geraSolicitacao implements AcaoRotinaJava {

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
        BigDecimal codTipOper = new BigDecimal(contextoAcao.getParam("CODTIPOPER").toString());

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

            codParc = new BigDecimal(linha.getCampo("CODPARC").toString());
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

            DynamicVO modeloNotaVO = cabDAO.findOne("NUNOTA = ?", modeloNota);

            cabVO = cabDAO.findOne("CODEMP = ? AND CODTIPOPER = ? AND CODPARC = ? AND STATUSNOTA != 'L' AND DTNEG = ? AND CODUSU = ? AND CAST(OBSERVACAO AS VARCHAR(1000)) = ?"
                    ,codEmp,codTipOper,codParc,TimeUtils.getNow("dd/MM/yyyy"), codUsuario, obs);

            if (cabVO == null) {
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

                nuNotas.add(cabVO.asBigDecimal("NUNOTA"));
            }

            if (cabVO.asTimestamp("DTPREVENT") != null && dtPrevEnt.before(cabVO.asTimestamp("DTPREVENT"))) {
                cabDAO.prepareToUpdateByPK(cabVO.asBigDecimalOrZero("NUNOTA"))
                        .set("DTPREVENT", dtPrevEnt)
                        .update();
            }

            BigDecimal qtdNeg = new BigDecimal (linha.getCampo("SUGERIDO").toString());

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
                iteDAO.create()
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
                        .set("CODLOCALORIG", proVO.asBigDecimalOrZero("CODLOCALPADRAO"))
                        .set("ATUALESTOQUE", BigDecimal.ZERO)
                        .set("CODVOL", proVO.asString("CODVOL"))
                        .set("DTINICIO", linha.getCampo("DTINI"))
                        .save();

                validaIns = true;
            } else {
                if (iteVO != null && iteVO.asBigDecimal("QTDNEG").compareTo(qtdNeg)<0){
                    iteDAO.prepareToUpdate(iteVO)
                            .set("QTDNEG",qtdNeg)
                            .set("VLRTOT",qtdNeg.multiply(custo))
                            .update();
                    validaIns = true;
                }
            }
        }

        if (validaIns) {
            msgRetorno = "Pedidos inseridos/atualizados. ".concat(cabVO.asBigDecimalOrZero("NUNOTA").toString());
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
            impHelper.totalizarNota(nuNota);
            impHelper.salvarNota();
//            ConfirmacaoNotaHelper.confirmarNota(nuNota, bRegras, false);
        }
    }
}
