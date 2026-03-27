package br.com.sankhya.bhz.producao;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.Report;
import br.com.sankhya.modelcore.util.ReportManager;
import com.sankhya.util.BigDecimalUtil;
import com.sankhya.util.TimeUtils;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperPrint;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class EventoDisparoEmailAceiteAtividade implements EventoProgramavelJava {

    private static final BigDecimal COD_MODELO_EMAIL = BigDecimal.valueOf(2);
    private static final BigDecimal COD_RELATORIO_PADRAO = BigDecimal.valueOf(96);

    private static final String SQL_DADOS = "sql/dadosEmailAtividadeAceite.sql";
    private static final String NOME_PDF = "FICHAPRODUCAOOP.pdf";

    private static final ThreadLocal<Set<String>> DISPAROS_NA_THREAD =
            ThreadLocal.withInitial(HashSet::new);

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception { }

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {
        final DynamicVO newVO = (DynamicVO) event.getVo();

        final String tipo = newVO.asString("TIPO");
        if (tipo == null || !"N".equalsIgnoreCase(tipo.trim())) {
            return;
        }

        final BigDecimal idiAtv = newVO.asBigDecimalOrZero("IDIATV");
        if (idiAtv == null || idiAtv.compareTo(BigDecimal.ZERO) == 0) {
            return;
        }

        Timestamp dhInicio = newVO.asTimestamp("DHINICIO");
        if (dhInicio == null) {
            dhInicio = newVO.asTimestamp("DHFINAL");
        }
        if (dhInicio == null) {
            dhInicio = TimeUtils.getNow();
        }

        final String flagKey = "BHZ_EMAIL_TPRIATV_" + idiAtv + "_" + dhInicio.getTime();
        Set<String> flags = DISPAROS_NA_THREAD.get();
        if (flags.contains(flagKey)) {
            return;
        }
        flags.add(flagKey);

        try {
            final BigDecimal usuRemet = getUsuarioRemetente();
            dispararEmailComAnexo(idiAtv, usuRemet);
        } finally {
            try {
                Set<String> f = DISPAROS_NA_THREAD.get();
                f.remove(flagKey);
                if (f.isEmpty()) {
                    DISPAROS_NA_THREAD.remove();
                }
            } catch (Exception ignored) { }
        }
    }

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception { }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception { }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception { }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception { }

    @Override
    public void beforeCommit(TransactionContext tranCtx) throws Exception { }

    private void dispararEmailComAnexo(BigDecimal idiAtv, BigDecimal usuRemet) throws Exception {

        JapeWrapper axmDAO = JapeFactory.dao("AnexoPorMensagem"); // TMDAXM
        JapeWrapper amgDAO = JapeFactory.dao("AnexoMensagem");    // TMDAMG
        JapeWrapper memDAO = JapeFactory.dao("ModeloEmail");      // TSIMEM

        EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = dwfFacade.getJdbcWrapper();

        DynamicVO memVO = memDAO.findOne("CODMODELO = ?", COD_MODELO_EMAIL);
        if (memVO == null) {
            return;
        }

        BigDecimal codRelatorio = COD_RELATORIO_PADRAO;
        if (memVO.asBigDecimalOrZero("AD_CODREL").compareTo(BigDecimal.ZERO) != 0) {
            codRelatorio = memVO.asBigDecimalOrZero("AD_CODREL");
        }

        ResultSet rs = null;

        try {
            jdbc.openSession();

            NativeSql sql = new NativeSql(jdbc);
            sql.loadSql(EventoDisparoEmailAceiteAtividade.class, SQL_DADOS);
            sql.setNamedParameter("IDIATV", idiAtv);

            rs = sql.executeQuery();

            while (rs.next()) {

                String emailsRaw = rs.getString("EMAIL");
                if (emailsRaw == null || emailsRaw.trim().isEmpty()) {
                    continue;
                }

                BigDecimal idiproc = rs.getBigDecimal("IDIPROC");
                BigDecimal nunota = rs.getBigDecimal("NUNOTA");
                String descrProd = rs.getString("DESCRPROD");
                BigDecimal qtdProduzir = rs.getBigDecimal("QTDPRODUZIR");
                String descrAtv = rs.getString("DESCRICAO");

                String idiprocStr = idiproc != null ? idiproc.toString() : "";
                String nunotaStr = nunota != null ? nunota.toString() : "";
                String descrStr = descrProd != null ? descrProd : "";
                String qtdStr = qtdProduzir != null ? qtdProduzir.toString() : "";
                String descrAtvStr = descrAtv != null ? descrAtv : "";

                String assunto = memVO.asString("ASSUNTO");
                if (assunto == null) {
                    assunto = "";
                }
                assunto = assunto
                        .replace("&amp;", "&")
                        .replace(":IDIPROC", idiprocStr)
                        .replace(":DESCRICAO", descrAtvStr);

                char[] conteudo = memVO.asClob("CONTEUDO");
                String msg = conteudo != null ? new String(conteudo) : "";
                msg = msg
                        .replace("&amp;", "&")
                        .replace(":IDIPROC", idiprocStr)
                        .replace(":NUNOTA", nunotaStr)
                        .replace(":DESCRPROD", descrStr)
                        .replace(":QTDPRODUZIR", qtdStr);

                String[] emails = emailsRaw.split(";");
                for (String email : emails) {
                    if (email == null || email.trim().isEmpty()) {
                        continue;
                    }

                    DynamicVO filaVO = (DynamicVO) dwfFacade.getDefaultValueObjectInstance(DynamicEntityNames.FILA_MSG);

                    filaVO.setProperty("EMAIL", email.trim());
                    filaVO.setProperty("CODCON", BigDecimal.ZERO);
                    filaVO.setProperty("CODMSG", null);
                    filaVO.setProperty("STATUS", "Pendente");
                    filaVO.setProperty("TIPOENVIO", "E");
                    filaVO.setProperty("MIMETYPE", "text/html");
                    filaVO.setProperty("TIPODOC", "N");
                    filaVO.setProperty("MAXTENTENVIO", BigDecimalUtil.valueOf(3));
                    // filaVO.setProperty("CODSMTP", memVO.asBigDecimal("CODSMTP"));
                    filaVO.setProperty("ASSUNTO", assunto);
                    filaVO.setProperty("MENSAGEM", msg.toCharArray());
                    filaVO.setProperty("CODUSUREMET", usuRemet != null ? usuRemet : BigDecimal.ZERO);
                    filaVO.setProperty("DTENTRADA", TimeUtils.getNow());

                    dwfFacade.createEntity(DynamicEntityNames.FILA_MSG, (EntityVO) filaVO);
                    BigDecimal codFila = filaVO.asBigDecimal("CODFILA");

                    Report report = ReportManager.getInstance().getReport(codRelatorio, dwfFacade);

                    HashMap<String, Object> reportParams = new HashMap<String, Object>();
                    reportParams.put("IDIPROC", idiproc);
                    reportParams.put("PK_IDIPROC", idiproc);
                    reportParams.put("PDIR_MODELO", "/home/mgeweb/repositorio/impressao/");

                    JasperPrint jasperPrint = report.buildJasperPrint(reportParams, jdbc.getConnection());
                    byte[] pdf = JasperExportManager.exportReportToPdf(jasperPrint);

                    DynamicVO amgVO = amgDAO.create()
                            .set("TIPO", "application/pdf")
                            .set("NOMEARQUIVO", NOME_PDF)
                            .set("ANEXO", pdf)
                            .save();

                    axmDAO.create()
                            .set("CODFILA", codFila)
                            .set("NUANEXO", amgVO.asBigDecimal("NUANEXO"))
                            .save();
                }
            }

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception ignored) { }
            try {
                jdbc.closeSession();
            } catch (Exception ignored) { }
        }
    }

    private BigDecimal getUsuarioRemetente() {
        try {
            AuthenticationInfo ai = AuthenticationInfo.getCurrent();
            if (ai != null && ai.getUserID() != null) {
                return ai.getUserID();
            }
        } catch (Exception ignored) { }
        return BigDecimal.ZERO;
    }

    private String getPdirModelo() {
        try {
            String baseDir = System.getProperty("user.dir");
            return baseDir != null ? baseDir : "";
        } catch (Exception e) {
            return "";
        }
    }
}