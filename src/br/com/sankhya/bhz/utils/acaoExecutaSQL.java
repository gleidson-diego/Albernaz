package br.com.sankhya.bhz.utils;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class acaoExecutaSQL implements AcaoRotinaJava {
    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        JdbcWrapper jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
        jdbc.openSession();
        NativeSql sql = new NativeSql(jdbc);
        sql.executeUpdate("INSERT INTO TGFCTE (DTCONTAGEM, CODEMP, CODLOCAL, CODPROD, CONTROLE, QTDEST, CODVOL, CODPARC, TIPO, SEQUENCIA, QTDESTUNCAD) VALUES ('14/12/2025', 10, 3577, 200406200 ,' ',114139,'UN',0,'P',3579,114139");
    }
}
