package br.com.sankhya.bhz.utils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.dao.EntityDAO;
import br.com.sankhya.jape.dao.EntityPropertyDescriptor;
import br.com.sankhya.jape.dao.PersistentObjectUID;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

public class duplicarRegistro {

    public static DynamicVO duplicaRegistroVO(DynamicVO voOrigem, String entidade, Map<String, Object> map) throws Exception {
        EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
        EntityDAO rootDAO = dwfFacade.getDAOInstance(entidade);
        DynamicVO destinoVO = voOrigem.buildClone();
        limparPk(destinoVO, rootDAO);
        if (map != null)
            for (String campo : map.keySet())
                destinoVO.setProperty(campo, map.get(campo));
        PersistentLocalEntity createEntity = dwfFacade.createEntity(entidade, (EntityVO) destinoVO);
        return (DynamicVO) createEntity.getValueObject();
    }

    private static void limparPk(DynamicVO vo, EntityDAO rootDAO) throws Exception {
        PersistentObjectUID objectUID = rootDAO.getSQLProvider().getPkObjectUID();
        EntityPropertyDescriptor[] pkFields = objectUID.getFieldDescriptors();
        for (EntityPropertyDescriptor pkField : pkFields) {
            vo.setProperty(pkField.getField().getName(), null);
        }
    }

    public static Timestamp getDataMaxOper(BigDecimal codigoTipoOperacao) throws Exception {
        AcessoBanco acessoBanco = null;
        try{
            acessoBanco = new AcessoBanco();
            return acessoBanco.findOne("SELECT MAX(DHALTER) AS DT FROM TGFTOP WHERE CODTIPOPER = " + codigoTipoOperacao)
                    .getTimestamp("DT");
        }finally {
            acessoBanco.closeSession();
        }
    }

    public static Timestamp getDataMaxTipoNeg(BigDecimal codigoTipoNegociacao) throws Exception {
        AcessoBanco acessoBanco = null;
        try{
            acessoBanco = new AcessoBanco();
            return acessoBanco.findOne("SELECT MAX(DHALTER) AS DT FROM TGFTPV WHERE CODTIPVENDA = " + codigoTipoNegociacao)
                    .getTimestamp("DT");
        }finally {
            acessoBanco.closeSession();
        }
    }
}
