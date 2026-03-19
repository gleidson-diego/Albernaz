select
                    ITE.NUNOTA NUNOTARET,
                    ITE.SEQUENCIA SEQUENCIARET,
                    ITE.CODPROD,
                    ITE.VLRUNIT,
                    ITE.CODVOL,
                    ITE.QTDNEG,
                    SUM(NVL(VAR.QTDATENDIDA,0)) QTDRETORNO,
                    ITE.CONTROLE,
                    ITE.CODLOCALORIG
     from tgfcab cab
inner join tgfite ITE on cab.nunota = ITE.nunota
left join tgfvar var on var.nunota  = cab.nunota AND var.sequencia = ITE.sequencia
 where codtipoper = 102 and cab.NUNOTA IN (SELECT DISTINCT V.NUNOTA FROM TGFCAB C
                                                                            INNER JOIN TGFVAR V ON V.NUNOTA = C.NUNOTA

                                      WHERE C.NUNOTA = :NUNOTA)
            AND ITE.AD_CONSIGNADO = 'S'
                GROUP BY ITE.NUNOTA,
                         ITE.SEQUENCIA,
                         ITE.CODPROD,
                         ITE.VLRUNIT,
                         ITE.CODVOL,
                         ITE.QTDNEG,
                         ITE.CONTROLE,
                         ITE.CODLOCALORIG