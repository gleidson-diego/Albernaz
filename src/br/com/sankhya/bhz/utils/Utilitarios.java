package br.com.sankhya.bhz.utils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.EntityDAO;
import br.com.sankhya.jape.dao.EntityPropertyDescriptor;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.dao.PersistentObjectUID;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.util.JapeSessionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.BarramentoRegra;
import br.com.sankhya.modelcore.comercial.CentralFinanceiro;
import br.com.sankhya.modelcore.comercial.LiberacaoSolicitada;
import br.com.sankhya.modelcore.comercial.centrais.CACHelper;
import br.com.sankhya.modelcore.comercial.impostos.ImpostosHelpper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.portaria.model.helper.NotaHelper;
import com.sankhya.util.BigDecimalUtil;
import com.sankhya.util.TimeUtils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.Map;

public class Utilitarios {


    public static BigDecimal ONE_HUNDRED = new BigDecimal(100);
    public static MathContext mathContext = new MathContext(4, RoundingMode.HALF_EVEN);

    public static void recalculaImpostosNota(BigDecimal nuNota) throws Exception {
        ImpostosHelpper impostohelp = new ImpostosHelpper();
        impostohelp.setForcarRecalculo(true);
        impostohelp.setSankhya(false);
        impostohelp.calcularImpostos(nuNota);
    }

    public static void confirmarNota(BigDecimal nuNota) throws Exception {
        String toResult="";
        CACHelper cacHelper = new CACHelper();

        BarramentoRegra barramento = BarramentoRegra.build(CACHelper.class,
                "regrasConfirmacaoCAC.xml", AuthenticationInfo.getCurrent());
        cacHelper.confirmarNota(nuNota, barramento, false);


        if (barramento.getLiberacoesSolicitadas().size() == 0 &&
                barramento.getErros().size() == 0) {
            System.out.println("Nota Confirmada " + nuNota + "");

        } else {
            if (barramento.getErros().size() > 0) {
                System.out.println("Erro na confirma��o " +
                        nuNota);

                for (Exception e : barramento.getErros()) {
                    toResult =
                            e.getMessage();
                    break;
                }
            }

            if (barramento.getLiberacoesSolicitadas().size() > 0) {
                System.out.println("Erro na confirma��o " + nuNota
                        + ". Foi solicitada libera��es");
                toResult = "Libera��es solicitadas - \n";
                for (LiberacaoSolicitada e :
                        barramento.getLiberacoesSolicitadas()) {
                    toResult += "Evento: "
                            + e.getEvento() + (e.getDescricao() != null ? " Descri��o:  "
                            + e.getDescricao() + "\n" : "\n");
                    break;
                }

            }

        }
        System.out.println(toResult);
    }
    public static void refazerFinanceiro(BigDecimal nuNota) throws Exception {
        //Intrinsics.checkParameterIsNotNull(nuNota, "nuNota");
        JapeSessionContext.putProperty("br.com.sankhya.com.CentralCompraVenda", Boolean.TRUE);
        JapeSessionContext.putProperty("br.com.sankhya.com.CentralCompraVenda", Boolean.TRUE);
        JapeSessionContext.putProperty("ItemNota.incluindo.alterando.pela.central", Boolean.TRUE);
        JapeSessionContext.putProperty("calcular.outros.impostos", "false");
        CentralFinanceiro financeiro = new CentralFinanceiro();
        financeiro.inicializaNota(nuNota);
        financeiro.refazerFinanceiro();
    }

    public static void duplicaRegistroTAB(ResultSet registro, String tabela, String pk, Map<String, Object> map) throws Exception {
        ResultSetMetaData metaData = registro.getMetaData();
        int columnCount = metaData.getColumnCount();
        AcessoBanco acessobanco = new AcessoBanco();
        acessobanco.openSession();
        try{
            String columnName = pk ;
            String values = "?";
            int tamanho = map.size();
            //ErroUtils.disparaErro(String.valueOf(tamanho));
            for (int i = 2; i <= tamanho; i++){
                values = values + ",?";

            }
            String dados = null;
            for(String campo : map.keySet()){
                campo = map.get(campo).toString();

                //campo = StringUtils.formatNumeric("0000000", map.get(campo));

                if(null != dados) {
                    dados = dados + campo + ",";
                }else{
                    dados = campo + ",";
                }
            }
            columnCount = columnCount+tamanho;
            for (int i = 1; i <= columnCount; i++) {
                columnName = columnName + "," + metaData.getColumnName(i);
                values = values + ",?";
                dados = dados + registro.getString(i)+(i < columnCount ? "," : "");
            }
            ErroUtils.disparaErro("INSERT INTO "+tabela+"("+columnName+") VALUES("+values+")"+dados);
            //ErroUtils.disparaErro(dados);
            acessobanco.update("INSERT INTO "+tabela+"("+columnName+") VALUES("+values+")",dados);
        }finally {
            acessobanco.closeSession();
        }

    }
    public static DynamicVO duplicaRegistroVO(DynamicVO voOrigem, String entidade) throws Exception {
        return duplicaRegistroVO( voOrigem,  entidade, null);
    }

    public static DynamicVO duplicaRegistroVO(DynamicVO voOrigem, String entidade, Map<String, Object> map) throws Exception {
        EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
        EntityDAO rootDAO = dwfFacade.getDAOInstance(entidade);
        DynamicVO destinoVO = voOrigem.buildClone();
        limparPk(destinoVO, rootDAO);
        if (map != null)
            for (String campo : map.keySet())
                destinoVO.setProperty(campo, map.get(campo));
        PersistentLocalEntity createEntity = dwfFacade.createEntity(entidade, (EntityVO) destinoVO);
        DynamicVO save = (DynamicVO) createEntity.getValueObject();
        return save;
    }


    private static void limparPk(DynamicVO vo, EntityDAO rootDAO) throws Exception {
        PersistentObjectUID objectUID = rootDAO.getSQLProvider().getPkObjectUID();
        EntityPropertyDescriptor[] pkFields = objectUID.getFieldDescriptors();
        for (EntityPropertyDescriptor pkField : pkFields) {
            vo.setProperty(pkField.getField().getName(), null);
        }
    }

    public static BigDecimal buscaLocalProdutoEmpresa( BigDecimal produto) throws Exception {
        JapeWrapper produtoDAO = JapeFactory.dao("Produto"); //TGFPRO
        DynamicVO produtoVO = produtoDAO.findByPK(produto);

        if (produtoVO != null) {
            return BigDecimalUtil.getValueOrZero(produtoVO.asBigDecimalOrZero("CODLOCALPADRAO") );
        } else
            return BigDecimal.ZERO;
    }
    public static Timestamp getDataMaxTipoOper(BigDecimal codTipOper) throws Exception {
        AcessoBanco acessoBanco = new AcessoBanco();
        try{
            return acessoBanco.findOne("SELECT MAX(DHALTER) AS DT FROM TGFTOP WHERE CODTIPOPER = ? ",codTipOper)
                    .getTimestamp("DT");
        }finally {
            acessoBanco.closeSession();
        }
    }

    public static Timestamp getDataMaxTipVenda(BigDecimal codTipVenda) throws Exception {
        AcessoBanco acessoBanco = new AcessoBanco();
        try{
            return acessoBanco.findOne("SELECT MAX(DHALTER) AS DT FROM TGFTPV WHERE CODTIPVENDA = ? ",codTipVenda)
                    .getTimestamp("DT");
        }finally {
            acessoBanco.closeSession();
        }
    }

    public static Timestamp getDataMaxTipvenda(BigDecimal codTipvenda) throws Exception {
        AcessoBanco acessoBanco = new AcessoBanco();
        try{
            return acessoBanco.findOne("SELECT MAX(DHALTER) AS DT FROM TGFTPV WHERE CODTIPVENDA = ? ",codTipvenda)
                    .getTimestamp("DT");
        }finally {
            acessoBanco.closeSession();
        }
    }

    public static void incluirAvisoSistema(BigDecimal codusuremetente, String titulo, String descricao, String solucao, String tipoDest, BigDecimal destinatario, BigDecimal importacia) throws Exception {
        JapeSession.SessionHandle hnd = null;
        JdbcWrapper jdbc = null;
        EntityFacade dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
        jdbc = dwfEntityFacade.getJdbcWrapper();
        BigDecimal codgrupo = null;
        BigDecimal codusu = null;
        if ("G".equals(tipoDest)) {
            codgrupo = destinatario;
        } else {
            codusu = destinatario;
        }

        if (!verificarAvisoEnviado(jdbc, titulo, descricao, solucao, "AVISO FLUXO", importacia, codusu, codgrupo)) {
            try {
                JapeWrapper aviDAO = JapeFactory.dao("AvisoSistema"); //TSIAVI
                aviDAO.create()
                        .set("CODUSUREMETENTE", codusuremetente)
                        .set("TITULO", titulo)
                        .set("DESCRICAO", descricao)
                        .set("DHCRIACAO", TimeUtils.getNow())
                        .set("IDENTIFICADOR", "AVISO FLUXO")
                        .set("TIPO", "P")
                        .set("SOLUCAO", solucao)
                        .set("CODGRUPO", codgrupo)
                        .set("CODUSU", codusu)
                        .set("IMPORTANCIA", importacia)
                        .save();

            } catch (Exception var14) {
                String msgErro = String.valueOf(var14);
                if (var14.toString().length() > 1800) {
                    msgErro = var14.toString().substring(0, 1800);
                }

                throw new MGEModelException("Erro incluir aviso no sistema: " + msgErro);
            }
        }
    }

    private static boolean verificarAvisoEnviado(JdbcWrapper jdbc, String titulo, String descricao, String solucao, String identificador, BigDecimal importancia, BigDecimal codusu, BigDecimal codgrupo) throws Exception {
        NativeSql sqlMsgEnviada = new NativeSql(jdbc);
        sqlMsgEnviada.appendSql("SELECT DISTINCT 1");
        sqlMsgEnviada.appendSql("  FROM TSIAVI");
        sqlMsgEnviada.appendSql(" WHERE (DHCRIACAO) = (GETDATE())");
        sqlMsgEnviada.appendSql("   AND LEN(TITULO) = LEN(:TITULO)");
        sqlMsgEnviada.appendSql("   AND LEN(DESCRICAO) = LEN(:DESCRICAO)");
        sqlMsgEnviada.appendSql("   AND LEN(SOLUCAO) = LEN(:SOLUCAO)");
        sqlMsgEnviada.appendSql("   AND CODUSU = ISNULL(:CODUSU,CODUSU)");
        sqlMsgEnviada.setNamedParameter("TITULO", titulo);
        sqlMsgEnviada.setNamedParameter("DESCRICAO", descricao);
        sqlMsgEnviada.setNamedParameter("SOLUCAO", solucao);
        sqlMsgEnviada.setNamedParameter("CODUSU", codusu);
        ResultSet rsMsgEnviada = sqlMsgEnviada.executeQuery();
        return rsMsgEnviada.next();
    }
    public static void adicionarFilaEmail(String assunto, String mensagem,String destinatarios, BigDecimal conta) throws Exception {
        JapeWrapper emailDAO = JapeFactory.dao("MSDFilaMensagem");
        Timestamp dtAgora = new Timestamp(System.currentTimeMillis());
        String emails[] = destinatarios.split(";");
        //BigDecimal smtp = (BigDecimal) MGECoreParameter.getParameter("BH_CONTASMTPP");
        for (String email : emails) {
            FluidCreateVO creEmail = emailDAO.create();
            creEmail.set("CODCON", BigDecimal.ZERO);
            creEmail.set("CODSMTP", conta);
            creEmail.set("EMAIL", email);
            creEmail.set("ASSUNTO", assunto);
            creEmail.set("MENSAGEM", mensagem.toCharArray());
            creEmail.set("DTENTRADA", dtAgora);
            creEmail.set("STATUS", "Pendente");
            creEmail.set("TIPOENVIO", "E");
            creEmail.set("MAXTENTENVIO", BigDecimalUtil.valueOf(3L));
            creEmail.save();
        }
    }
    public static void totalizar(BigDecimal nuNota) throws Exception{
        JapeSessionContext.putProperty("br.com.sankhya.com.CentralCompraVenda", (Object)Boolean.TRUE);
        ImpostosHelpper impHelper = new ImpostosHelpper();
        impHelper.totalizarNota(nuNota);
        impHelper.salvarNota();
        recalculaImpostosNota(nuNota);
    }

    public static void gerarLote(BigDecimal nuNota) throws Exception {
        NotaHelper.gerarLote(nuNota);
    }

}


