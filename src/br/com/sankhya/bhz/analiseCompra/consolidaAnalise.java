package br.com.sankhya.bhz.analiseCompra;

import br.com.sankhya.bhz.utils.ErroUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;

public class consolidaAnalise implements AcaoRotinaJava {
    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        JapeWrapper imrpDAO = JapeFactory.dao("AD_BHZIMRP");

        EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = dwfFacade.getJdbcWrapper();

        jdbc.openSession();
        NativeSql sqlCOMMIT = new NativeSql(jdbc);
        sqlCOMMIT.appendSql("COMMIT");

        NativeSql sqlDEL = new NativeSql(jdbc);
        sqlDEL.loadSql(consolidaAnalise.class, "sql/deletaAnalise.sql");
        sqlDEL.executeUpdate();

        NativeSql sqlINS = new NativeSql(jdbc);
        sqlINS.loadSql(consolidaAnalise.class, "sql/consolidaAnalise.sql");
        sqlINS.executeUpdate();

        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT * FROM AD_BHZIMRP ORDER BY CODEMP, CODPRODMP, LIMITECOMPRA");
        ResultSet resultSet = sql.executeQuery();

        BigDecimal acumulado = BigDecimal.ZERO;
        BigDecimal sugestao = BigDecimal.ZERO;
        BigDecimal codprod = BigDecimal.ZERO;
        BigDecimal saldo = BigDecimal.ZERO;
        BigDecimal qtdPend = BigDecimal.ZERO;
        BigDecimal qtdEst = BigDecimal.ZERO;

        while (resultSet.next()){

            if(codprod.compareTo(resultSet.getBigDecimal("CODPRODMP"))!=0){

                acumulado = BigDecimal.ZERO;
                saldo = BigDecimal.ZERO;
                sugestao = BigDecimal.ZERO;
                codprod = resultSet.getBigDecimal("CODPRODMP");
                qtdPend = resultSet.getBigDecimal("QTDPEND");
                qtdEst = resultSet.getBigDecimal("QTDEST");

                saldo = resultSet.getBigDecimal("QTDNEG");

                acumulado = saldo.subtract(resultSet.getBigDecimal("QTDEST")
                        .add(qtdPend));

                if(acumulado.compareTo(BigDecimal.ZERO)>0) {
                    if(resultSet.getBigDecimal("ESTMIN").compareTo(BigDecimal.ZERO)>0){
                        BigDecimal estMin = resultSet.getBigDecimal("ESTMIN");
                        acumulado = acumulado.add(estMin);
                    }
                    if(resultSet.getBigDecimal("AGRUPCOMPMINIMO").compareTo(BigDecimal.ZERO)>0){
                        BigDecimal agrupamentoMin = resultSet.getBigDecimal("AGRUPCOMPMINIMO");
                        if(acumulado.compareTo(agrupamentoMin)<0){
                            acumulado = agrupamentoMin;
                        }
                    }
                    if(resultSet.getBigDecimal("QTDAGRUP").compareTo(BigDecimal.ZERO)>0){
                        BigDecimal multiplo = resultSet.getBigDecimal("QTDAGRUP");
                        sugestao = acumulado.divide(multiplo,0, RoundingMode.CEILING).multiply(multiplo);
                    } else {
                        sugestao = acumulado;
                    }
                }


            } else {
                qtdEst = qtdEst.subtract(saldo).add(qtdPend).add(sugestao);
                qtdPend = resultSet.getBigDecimal("QTDPEND").subtract(qtdPend);

                saldo = resultSet.getBigDecimal("QTDNEG");
                sugestao = saldo.subtract(qtdEst);

                if(sugestao.compareTo(BigDecimal.ZERO)>0){
                    if(resultSet.getBigDecimal("QTDAGRUP").compareTo(BigDecimal.ZERO)>0){
                        BigDecimal multiplo = resultSet.getBigDecimal("QTDAGRUP");
                        sugestao = sugestao.subtract(qtdPend).divide(multiplo,0, RoundingMode.CEILING).multiply(multiplo);
                    }
                    if(sugestao.add(resultSet.getBigDecimal("ESTMIN")).compareTo(BigDecimal.ZERO)>0){
                        sugestao = sugestao.subtract(qtdPend).add(resultSet.getBigDecimal("ESTMIN"));
                    }
                }
                acumulado = acumulado.subtract(qtdPend).add(saldo);

                if(acumulado.add(resultSet.getBigDecimal("ESTMIN")).compareTo(BigDecimal.ZERO)>0){
                    acumulado = acumulado.add(resultSet.getBigDecimal("ESTMIN"));
                }
            }

            if(sugestao.compareTo(BigDecimal.ZERO)<0){
                sugestao = BigDecimal.ZERO;
            }

            NativeSql sqlUPD = new NativeSql(jdbc);
            sqlUPD.loadSql(consolidaAnalise.class, "sql/updateAnalise.sql");
            sqlUPD.setNamedParameter("ACUMULADO",acumulado);
            sqlUPD.setNamedParameter("SUGERIDO",sugestao);
            sqlUPD.setNamedParameter("QTDEST",qtdEst);
            sqlUPD.setNamedParameter("QTDPEND",qtdPend);
            sqlUPD.setNamedParameter("CODPRODMP",resultSet.getBigDecimal("CODPRODMP"));
            sqlUPD.setNamedParameter("CODEMP",resultSet.getBigDecimal("CODEMP"));
            sqlUPD.setNamedParameter("NUMPS",resultSet.getBigDecimal("NUMPS"));
            sqlUPD.setNamedParameter("DTINI",resultSet.getTimestamp("DTINI"));
            sqlUPD.executeUpdate();


        }
        sqlCOMMIT.executeUpdate();
        jdbc.closeSession();
    }
}
