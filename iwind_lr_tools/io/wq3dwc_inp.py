
from typing import List

from .common import Node, DataFrameNode
from .master_input import generate_parse

card_info_dsl = """
C01 NWQTS  IBINDMP   IANOX IDNOTRVA
C02 Bc Bd Bg ROC LOC LDOC RDC ROP LOP LDOP RDP PO4t RPON LON LDON RDN NH4 NO3 SU SA COD DO TAM FCB DSE PSE
C03 IWQDT   IWQBEN  IWQSI   IWQFCB  IWQSRP  IWQSTOX  IWQKA IWQVLIM
C04 IWQZ    IWQNC   IWQRST  LDMWQ   FNFIX   WQGNC_T  RFPO4 RMNLIMin   BEN_UPKAKE
C05 IWQICI  IWQPSL
C06 IWQTS   TWQTSB  TWQTSE  WQTSDT  NWQTSDTBIN
C07 I   J
C08 KHNc   KHNd   KHNg    KHNm   KHPc   KHPd   KHPg   KHPm   KHS   STOX
C09 KeTSS  KeChl  CChlc  CChld  CChlg  CChlm  DOPTc  DOPTd DOPTg DOPTm ISCOLOR	KEOM
C10  I0    IsMIN  FD     CIa    CIb    CIc    CIm    Rea   PARadj  
C11 TMc1  TMc2   TMd1   TMd2   TMg1   TMg2   TMm1   TMm2   TMp1   TMp2
C12 KTG1c KTG2c  KTG1d  KTG2d  KTG1g  KTG2g  KTG1m  KTG2m  KTG1p  KTG2p
C13 TRc   TRd   TRg   TRm   KTBc   KTBd   KTBg   KTBm
C14 FCRP   FCLP   FCDP   FCDc  FCDd   FCDg   KHRc  KHRd  KHRg
C15 FCRPm  FCLPm  FCDPm  FCDm  KHRm
C16 KRC     KLC     KDC     KRCalg   KLCalg    KDCalg    KDCalgm
C17 TRHDR   TRMNL   KTHDR   KTMNL   KHORDO  KHDNN   AANOX
C18 FPRP FPLP  FPDP  FPIP  FPRc  FPRd FPRg FPLc FPLd FPLg
C19 FPRPM  FPLPM  FPDPM  FPIPM   FPRm   FPLm   
C20 FPDc   FPDd   FPDg   FPDm    FPIc   FPId   FPIg   FPIm   KPO4P  FRAC_CLEAVE
C21 KRP    KLP    KDP    KRPalg  KLPalg KDPalg CPprm1 CPprm2 CPprm3 ILUX   APCINI   FLDOPPRE  RDPPRE
C22 FNRP  FNLP  FNDP  FNIP  FNRc  FNRd  FNRg FNLc  FNLd  FNLg
C23 FNRPM FNLPM  FNDPM  FNIPM   FNRm  FNLm
C24 FNDc  FNDd  FNDg  FNDm  FNIc  FNId  FNIg  FNIm  ANCc  ANCd  ANCg  ANCm
C25 ANDC    rNitM   KHNitDO KHNitN  TNit    KNit1   KNit2
C26 KRN     KLN     KDN     KRNalg  KLNalg  KDNalg
C27 FSPP    FSIP    FSPd    FSId    ASCd    KSAp    KSU     TRSUA   KTSUA
C28 AOCR    AONT    KRO     KTR     KHCOD   KCD     TRCOD   KTCOD AOCRpm AOCRrm
C29 KHbmf   BFTAM   Ttam    Ktam    TAMdmx  Kdotam  KFCB    TFCB
C30  Bc      Bd      Bg    ROC   LOC   LDOC  RDC  ROP  LOP   LDOP   RDP   PO4t    RPON   LON  LDON  RDN    NH4   NO3    SU      SA    COD     DO    TAM    FCB   DSE   PSE  Bm  Bmin Algaemin
C31  PMc  PMd  PMg  PMm  BMRc  BMRd  BMRg  BMRm PRRc  PRRd  PRRg  PRRm   Keb    Zgrazec  Zgrazed  Zgrazeg Bfishm Efishm Fgrazem1  Fgrazem2
C32 WSc     WSd     WSg     WSrp    WSlp    WSs     WSM   RNPREF
C33  FPO4    FNH4    FNO3    FSAD    FCOD    SOD     
C34_1 IWQPS   NPSTMSR
C34_2 I   J   K   N   PSQ     Bc      Bd      Bg      ROC    LOC    LDOC  RDC  ROP    LOP    LDOP   RDP    PO4t    RPON    LON   LDON RDN   NH4     NO3     SU      SA      COD     DO      TAM     FCB  DSE  PSE
C35 DSQ     Bc      Bd      Bg      ROC    LOC    LDOC  RDC   ROP    LOP    LDOP    RDP    PO4t    RPON    LON   LDON  RDN  NH4     NO3     SU      SA      COD     DO      TAM    FCB   DSE  PSE
C36          Bc      Bd      Bg    ROC    LOC    LDOC    RDC  ROP    LOP    LDOP    RDP    PO4t    RPON    LON   LDON  RDN  NH4     NO3     SU      SA      COD     DO      TAM    FCB   DSE  PSE
C37 filename comment
C38 WQKRDC  WQKRDN   WQKRDP    wQKRDCalg WQKRDNalg WQKRDPalg    WQKPDC  WQKPDN   WQKPDP WQKPTH WQKPLGT WQKPTM0 WQKPTMP WQKRSE  WQKRSE2  WQSEDOHF  WQSE2DOHF WQSESET
C39 threshold
C40 WR_ID   Bc     Bd      Bg      ROC   LOC    LDOC      RDC   ROP   LOP    LDOP    RDP     PO4     RPON    LON   LDON     RDN     NH4    NO3     SU       SA     COD      DO     TAM     FCB      DSE        PSE        PSE
"""

forward_lookup_map = {
    "C01": {
        "IANOX": ["C39"]
    },
    "C06": {
        "IWQTS": ["C07"],
    },
    "C34_1": {
        "IWQPS": ["C34_2"]
    }
}

length_map_init = {
    "C01": 1,
    "C02": 1,
    "C03": 1,
    "C04": 1,
    "C05": 1,
    "C06": 1,
    # C07 -> C06
    "C08": 1,
    "C09": 1,
    "C10": 1,
    "C11": 1,
    "C12": 1,
    "C13": 1,
    "C14": 1,
    "C15": 1,
    "C16": 1,
    "C17": 1,
    "C18": 1,
    "C19": 1,
    "C20": 1,
    "C21": 1,
    "C22": 1,
    "C23": 1,
    "C24": 1,
    "C25": 1,
    "C26": 1,
    "C27": 1,
    "C28": 1,
    "C29": 1,
    "C30": 1,
    "C31": 1,
    "C32": 1,
    "C33": 1,
    "C34_1": 1,
    # C34_2 -> C34_1
    "C35": 1,
    "C36": 1,
    "C37": 10,
    "C38": 1,
    # C39 -> C1
    # C40 -> efdc C07
}

parse = generate_parse(card_info_dsl, forward_lookup_map, length_map_init)
# C40 must be passed as extra_length_map

def parse_dep(data_map: List[Node]):
    node_list = data_map["efdc.inp"]
    nqwr = DataFrameNode.get_df_map(node_list)["C07"]["NQWR"].iloc[0]
    return dict(extra_length_map=dict(C40=nqwr))