CREATE OR REPLACE PROCEDURE ALEX.R0108_RATE_TEX_SYS_TEST
IS
   start_                NUMBER;
   end_                  NUMBER;
   end_2                 NUMBER;
   V_DATAFOUND           VARCHAR2 (25);
   V_LEN_CODE            NUMBER;
   V_EUR_RATE_NO_FR      NUMBER := 0.15; --ПЕРЕМЕННАЯ ОТСЕЧЕНИЯ НЕФРОДООПАСНЫХ НАПРАВЛЕНИЙ - ЕВРО ЦЕНТЫ
   V_LEN_CODE_COUNTRY    NUMBER;
   V_EUR_RATE_FR         NUMBER := 0.30; --ПЕРЕМЕННАЯ НАПРАВЛЕНИЙ С ВЫСОКИМ РИСКОМ ФРОДА - ЕВРО ЦЕНТЫ
   V_EUR_MAX_RATE_FR     NUMBER := 0.70; --ПЕРЕМЕННАЯ МАКС СТАВКИ ДЛЯ ОПРЕДЕЛЕНИЯ ФРОД НАПРАВЛЕНИЙ - ЕВРО ЦЕНТЫ
   V_EUR_AVG_PROC_FR     NUMBER := 1.2; --ПЕРЕМЕННАЯ ПРОЦЕНТА ВЫШЕ СРЕДНЕЙ СТАВКИ НА СТРАНУ
   V_DEL_USER_TIER       VARCHAR2 (50); --переменная для удаления кодов, использует таблицу в которую вносят данные пользователи
   V_UPD_ROUTE_IXTOOLS   DATE; -- переменная обновления данных в таблице МАРШРУТИЗАЦИИ С СИСТЕМЫ IxTools

   --=============================================================================



   CURSOR DATAFOUND        --наличие новіх данніх в таблице блокированіх кодов
   IS
        SELECT SUBSTR (CODE_, 1, 5)
          FROM ALEX.R0108_ISC_BLOCK_CODE_LOAD
         WHERE SUBSTR (CODE_, 1, 5) = 'CODE='
      GROUP BY SUBSTR (CODE_, 1, 5);


   CURSOR LEN_CODE -- определяем количество циклов для анализа блокированых кодов
   IS
      SELECT MAX (LENGTH (CODE_)) FROM ALEX.R0108_ISC_BLOCK_CODE;


   CURSOR LEN_CODE_COUNTRY -- определяем количество циклов для проставления стран
   IS
      SELECT MAX (LENGTH (T_CNP)) FROM ALEX.TABLE_NUM_PLAN2;



   CURSOR C_DEL_USER_TIER
   IS
        SELECT FK_NADR || FK_ORGA_OPER || FK_PROD || FK_TIER
          FROM ALEX.R0108_DEL_USER_TIER
      GROUP BY FK_NADR || FK_ORGA_OPER || FK_PROD || FK_TIER;



   CURSOR UPD_ROUTE_IXTOOLS
   IS
        SELECT T_ENDDATE
          FROM ALEX.R0108_ROUTE_IXTOOLS_LOAD
         WHERE T_ENDDATE IS NOT NULL
      GROUP BY T_ENDDATE;
BEGIN
   EXECUTE IMMEDIATE 'truncate table ALEX.R0108_RATE_SYS_UNIT_TEST';


   --UNIT TEST
   INSERT INTO ALEX.R0108_RATE_SYS_UNIT_TEST (SYSDATE_, NAME_STRING_, COUNT_)
      SELECT SYSDATE, 'START', 0 FROM DUAL;

   COMMIT;

   UPDATE alex.t_log_ex_proc
      SET ID_ = 'ERR'
    WHERE ID_ = 'new_tmp' AND NAME_PROC = 'R0108_RATE_TEX_SYS';

   COMMIT;

   INSERT INTO alex.t_log_ex_proc (EX_STRT_DATE,
                                   ID_,
                                   NAME_PROC,
                                   CLEAN)
        VALUES (SYSDATE,
                'new_tmp',
                'R0108_RATE_TEX_SYS',
                'E');

   COMMIT;

   --==========================================================================
   --
      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_RATE_SYS_AVG_TYPE';
   
      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_RATE_SYS';
   
      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_RATE_SYS_TMP';
   
      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_RATE_SYS_UNIT_TEST';



   --=============================================================================
   --обновляем коды блокированных кодов на ISC

   OPEN DATAFOUND;

   FETCH DATAFOUND INTO V_DATAFOUND;



   IF DATAFOUND%FOUND
   THEN
      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_ISC_BLOCK_CODE';

      INSERT INTO ALEX.R0108_ISC_BLOCK_CODE (CODE_, DATE_INS)
           SELECT SUBSTR (CODE_,
                          INSTR (CODE_, '=') + 3,
                          LENGTH (CODE_) - INSTR (CODE_, '=') + 1),
                  SYSDATE
             FROM ALEX.R0108_ISC_BLOCK_CODE_LOAD
            WHERE REGEXP_LIKE (SUBSTR (CODE_, 6), '^00[1-9]')
         GROUP BY SUBSTR (CODE_,
                          INSTR (CODE_, '=') + 3,
                          LENGTH (CODE_) - INSTR (CODE_, '=') + 1);

      COMMIT;

      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_ISC_BLOCK_CODE_LOAD';


      INSERT INTO ALEX.R0108_ISC_BLOCK_CODE_LOAD (CODE_)
         SELECT CODE_ FROM ALEX.R0108_ISC_BLOCK_CODE;

      COMMIT;

      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_ISC_BLOCK_CODE';


      --##########################################################################

      --ПРОЦЕДУРА ПРОСТАВЛЕНИЯ СТРАН

      OPEN LEN_CODE_COUNTRY;

      FETCH LEN_CODE_COUNTRY INTO V_LEN_CODE_COUNTRY;

      start_ := 0;
      end_ := V_LEN_CODE_COUNTRY;

      LOOP
         INSERT INTO ALEX.R0108_ISC_BLOCK_CODE (CODE_,
                                                DATE_INS,
                                                COUNTRY,
                                                NAME_DEST)
            SELECT RT1.CODE_,
                   SYSDATE,
                   TNP2.T_NETWORK_OPERATOR,
                   'BLOCK_CODE_ISC'
              FROM    ALEX.R0108_ISC_BLOCK_CODE_LOAD RT1
                   LEFT JOIN
                      ALEX.TABLE_NUM_PLAN2 TNP2
                   ON (SUBSTR (RT1.CODE_, 1, LENGTH (RT1.CODE_) - start_) =
                          TNP2.T_CNP)
             WHERE TNP2.T_NETWORK_OPERATOR IS NOT NULL;


         COMMIT;

         DELETE ALEX.R0108_ISC_BLOCK_CODE_LOAD
          WHERE CODE_ IN (  SELECT CODE_
                              FROM ALEX.R0108_ISC_BLOCK_CODE
                          GROUP BY CODE_);

         COMMIT;

         start_ := start_ + 1;

         IF (start_ >= end_)
         THEN
            EXIT;
         END IF;
      END LOOP;

      COMMIT;



      --##########################################################################

      INSERT INTO ALEX.R0108_ISC_BLOCK_CODE (CODE_,
                                             DATE_INS,
                                             COUNTRY,
                                             NAME_DEST)
         SELECT CODE_,
                SYSDATE,
                'unknown',
                'BLOCK_CODE_ISC'
           FROM ALEX.R0108_ISC_BLOCK_CODE_LOAD;

      COMMIT;

      CLOSE LEN_CODE_COUNTRY;

      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_ISC_BLOCK_CODE_LOAD';
   --      DELETE ALEX.R0113_TABLE_NUM_PLAN_ANY_NUM
   --       WHERE T_NETWORK_OPERATOR_OP = 'BLOCK_CODE_ISC';
   --
   --      COMMIT;
   --
   --      INSERT INTO ALEX.R0113_TABLE_NUM_PLAN_ANY_NUM (T_CNP,
   --                                                     DESTINATION,
   --                                                     T_NETWORK_OPERATOR,
   --                                                     T_NETWORK_OPERATOR_OP,
   --                                                     DATE_TIME_MOD,
   --                                                     TYPE_NETW)
   --           SELECT CODE_,
   --                  'BLOCK_CODE_ISC',
   --                  COUNTRY,
   --                  NAME_DEST,
   --                  DATE_INS,
   --                  'PRS'
   --             FROM ALEX.R0108_ISC_BLOCK_CODE
   --         GROUP BY CODE_,
   --                  COUNTRY,
   --                  NAME_DEST,
   --                  DATE_INS;
   --
   --      COMMIT;
   END IF;


   CLOSE DATAFOUND;

   --===========================================================================

   --БЛОК ЗАГРУЗКИ ТАБЛИЦЫ МАРШРУТИЗАЦИИ С СИСТЕМЫ IxTools
   --===========================================================================
   OPEN UPD_ROUTE_IXTOOLS;

   FETCH UPD_ROUTE_IXTOOLS INTO V_UPD_ROUTE_IXTOOLS;

   IF UPD_ROUTE_IXTOOLS%NOTFOUND
   THEN
      EXECUTE IMMEDIATE 'truncate table ALEX.R0108_ROUTE_IXTOOLS';

      UPDATE ALEX.R0108_ROUTE_IXTOOLS_LOAD
         SET T_ENDDATE = TRUNC (SYSDATE);

      COMMIT;

      DELETE ALEX.R0108_ROUTE_IXTOOLS_LOAD
       WHERE T_ROUTE_CLASS != 'OWN';

      COMMIT;

      DELETE ALEX.R0108_ROUTE_IXTOOLS_LOAD
       WHERE T_RANK NOT IN ('Rank 1', 'Rank 2', 'Rank 3', 'Rank 4');

      COMMIT;

      UPDATE ALEX.R0108_ROUTE_IXTOOLS_LOAD
         SET T_ACCOUNT = T_TRUNK;


      UPDATE ALEX.R0108_ROUTE_IXTOOLS_LOAD
         SET T_NOTES = T_PRODUCT;



      COMMIT;

      UPDATE ALEX.R0108_ROUTE_IXTOOLS_LOAD
         SET T_TRUNK = SUBSTR (T_TRUNK, INSTR (T_TRUNK, '/') + 1)
       WHERE SUBSTR (T_TRUNK, 1, INSTR (T_TRUNK, '/') - 1) != 'Commercial';

      COMMIT;

      UPDATE ALEX.R0108_ROUTE_IXTOOLS_LOAD
         SET T_TRUNK =
                SUBSTR (
                   SUBSTR (T_TRUNK, INSTR (T_TRUNK, '/') + 1),
                   1,
                     INSTR (SUBSTR (T_TRUNK, INSTR (T_TRUNK, '/') + 1), '-')
                   - 1)
       WHERE SUBSTR (T_TRUNK, 1, INSTR (T_TRUNK, '/') - 1) = 'Commercial';

      COMMIT;


      UPDATE ALEX.R0108_ROUTE_IXTOOLS_LOAD
         SET T_PRODUCT =
                SUBSTR (T_PRODUCT, 1, INSTR (T_PRODUCT, '-Voice') - 1);

      COMMIT;

      INSERT INTO ALEX.R0108_ROUTE_IXTOOLS (T_OPERATOR,
                                            T_DEST,
                                            T_COUNTRY,
                                            T_RANK,
                                            T_BEGIN_DATE)
           SELECT TR1.T_TRUNK,
                  TR1.T_PRODUCT,
                  TR2.T_NETWORK_OPERATOR,
                  TR1.T_RANK,
                  TO_DATE (TR1.T_BEGIN_DATE, 'DD.MM.YYYY')
             FROM    ALEX.R0108_ROUTE_IXTOOLS_LOAD TR1
                  LEFT JOIN
                     ALEX.TABLE_NUM_PLAN2 TR2
                  ON (TR1.T_PRODUCT = TR2.DESTINATION)
         GROUP BY TR1.T_TRUNK,
                  TR1.T_PRODUCT,
                  TR2.T_NETWORK_OPERATOR,
                  TR1.T_RANK,
                  TO_DATE (TR1.T_BEGIN_DATE, 'DD.MM.YYYY');

      COMMIT;


      UPDATE ALEX.R0108_ROUTE_IXTOOLS
         SET T_COUNTRY = 'Russia'
       WHERE REGEXP_LIKE (UPPER (T_DEST), 'RUSSIA') AND T_COUNTRY IS NULL;

      COMMIT;

      UPDATE ALEX.R0108_ROUTE_IXTOOLS
         SET T_COUNTRY = 'International Networks'
       WHERE     REGEXP_LIKE (UPPER (T_DEST), 'NAL NETWORKS')
             AND T_COUNTRY IS NULL;

      COMMIT;

      UPDATE ALEX.R0108_ROUTE_IXTOOLS
         SET T_UPD_DATE = SYSDATE;

      COMMIT;
   END IF;

   CLOSE UPD_ROUTE_IXTOOLS;

   --===========================================================================
   --переносим все действующие ставки из таблицы ставок системы интерконенкт

   INSERT INTO ALEX.R0108_RATE_SYS (FK_NADR,
                                    FK_ORGA_OPER,
                                    FK_PROD,
                                    FK_TIER,
                                    NAME,
                                    FK_CURR,
                                    UNIT_COST,
                                    FK_RATE_FED,
                                    UNIT_COST_USD,
                                    UNIT_COST_EUR,
                                    FK_NADR_MOD,
                                    ID_)
      SELECT FK_NADR,
             FK_ORGA_OPER,
             FK_PROD,
             FK_TIER,
             NAME,
             FK_CURR,
             UNIT_COST,
             FK_RATE_FED,
             UNIT_COST_USD,
             UNIT_COST_EUR,
             FK_NADR,
             'D'
        FROM ALEX.R0119_RATE_SYS
       WHERE     UNIT_COST_EUR >= V_EUR_RATE_NO_FR
             AND FK_TIER != 'CHROUTE'
             AND FK_PROD IN ('INTO', 'INTL');

   COMMIT;

   --UNIT TEST
   INSERT INTO ALEX.R0108_RATE_SYS_UNIT_TEST (SYSDATE_, NAME_STRING_, COUNT_)
      SELECT SYSDATE, 'R0108_RATE_SYS', COUNT (*) FROM ALEX.R0108_RATE_SYS;

   COMMIT;

   --END TEST UNIT


   --УДАЛЯЕМ НАПРАВЛЕНИЯ ПО КРИТЕРИЯМ ПОЛЬЗОВАТЕЛЕЙ


   OPEN C_DEL_USER_TIER;

   LOOP
      EXIT WHEN C_DEL_USER_TIER%NOTFOUND;

      FETCH C_DEL_USER_TIER INTO V_DEL_USER_TIER;

      --UNIT TEST
      INSERT INTO R0108_RATE_SYS_UNIT_TEST (NAME_STRING_)
         SELECT V_DEL_USER_TIER FROM DUAL;

      --END TEST UNIT
      DELETE ALEX.R0108_RATE_SYS
       WHERE REGEXP_LIKE (FK_NADR || FK_ORGA_OPER || FK_PROD || FK_TIER,
                          '' || V_DEL_USER_TIER || '');

      COMMIT;
   END LOOP;

   CLOSE C_DEL_USER_TIER;



   INSERT INTO ALEX.R0108_RATE_SYS_UNIT_TEST (SYSDATE_, NAME_STRING_, COUNT_)
      SELECT SYSDATE, 'R0108_RATE_SYS_186', COUNT (*)
        FROM ALEX.R0108_RATE_SYS;

   COMMIT;


   --УБИРАЕМ ПРЕФИКСЫ ДЛЯ АНАЛИЗА КОДОВ - ПРОСТАВЛЕНИЕ СТРАНЫ
   UPDATE ALEX.R0108_RATE_SYS
      SET FK_NADR_MOD = SUBSTR (FK_NADR, 2)
    WHERE SUBSTR (FK_NADR, 1, 1) IN ('D', 'T');

   COMMIT;

   --==========================================================================
   --УДАЛЕНИЕ БЛОКИРОВАНХ КОДОВ ИЗ ТАБЛИЦЫ СТАВОК

   OPEN LEN_CODE;

   FETCH LEN_CODE INTO V_LEN_CODE;

   start_ := 0;
   end_ := V_LEN_CODE;

   LOOP
      INSERT INTO ALEX.R0108_RATE_SYS_TMP (FK_NADR, FK_NADR_MOD)
         SELECT RC1.FK_NADR, BC1.CODE_
           FROM    ALEX.R0108_RATE_SYS RC1
                LEFT JOIN
                   ALEX.R0108_ISC_BLOCK_CODE BC1
                ON (SUBSTR (RC1.FK_NADR_MOD,
                            1,
                            LENGTH (RC1.FK_NADR_MOD) - start_) = BC1.CODE_)
          WHERE BC1.CODE_ IS NOT NULL;



      COMMIT;



      DELETE ALEX.R0108_RATE_SYS
       WHERE FK_NADR IN (  SELECT FK_NADR
                             FROM ALEX.R0108_RATE_SYS_TMP
                         GROUP BY FK_NADR);

      COMMIT;

      start_ := start_ + 1;

      IF (start_ >= end_)
      THEN
         EXIT;
      END IF;
   END LOOP;

   COMMIT;

   EXECUTE IMMEDIATE 'truncate table ALEX.R0108_RATE_SYS_TMP';

   CLOSE LEN_CODE;


   --UNIT TEST
   INSERT INTO ALEX.R0108_RATE_SYS_UNIT_TEST (SYSDATE_, NAME_STRING_, COUNT_)
      SELECT SYSDATE, 'DELETE_BLOCK_CODE_RATE_SYS_STR207', COUNT (*)
        FROM ALEX.R0108_RATE_SYS;

   COMMIT;

   --END TEST UNIT



   --ПРОЦЕДУРА ПРОСТАВЛЕНИЯ СТРАН

   OPEN LEN_CODE_COUNTRY;

   FETCH LEN_CODE_COUNTRY INTO V_LEN_CODE_COUNTRY;

   start_ := 0;
   end_2 := V_LEN_CODE_COUNTRY;

   LOOP
      INSERT INTO ALEX.R0108_RATE_SYS_TMP (FK_NADR,
                                           FK_ORGA_OPER,
                                           FK_PROD,
                                           FK_TIER,
                                           NAME,
                                           FK_CURR,
                                           UNIT_COST,
                                           FK_RATE_FED,
                                           RATE_LED,
                                           FK_NADR_MOD,
                                           UNIT_COST_USD,
                                           COUNTRY,
                                           UNIT_COST_EUR,
                                           ID_)
         SELECT RT1.FK_NADR,
                RT1.FK_ORGA_OPER,
                RT1.FK_PROD,
                RT1.FK_TIER,
                RT1.NAME,
                RT1.FK_CURR,
                RT1.UNIT_COST,
                RT1.FK_RATE_FED,
                RT1.RATE_LED,
                RT1.FK_NADR_MOD,
                RT1.UNIT_COST_USD,
                TNP2.T_NETWORK_OPERATOR,
                RT1.UNIT_COST_EUR,
                'N'
           FROM    ALEX.R0108_RATE_SYS RT1
                LEFT JOIN
                   ALEX.TABLE_NUM_PLAN2 TNP2
                ON (SUBSTR (RT1.FK_NADR_MOD,
                            1,
                            LENGTH (RT1.FK_NADR_MOD) - start_) = TNP2.T_CNP)
          WHERE RT1.ID_ = 'D' AND TNP2.T_NETWORK_OPERATOR IS NOT NULL;


      COMMIT;

      DELETE ALEX.R0108_RATE_SYS
       WHERE     ID_ = 'D'
             AND FK_NADR_MOD IN (SELECT FK_NADR_MOD
                                   FROM ALEX.R0108_RATE_SYS_TMP
                                  WHERE ID_ = 'N');

      COMMIT;

      start_ := start_ + 1;

      IF (start_ >= end_2)
      THEN
         EXIT;
      END IF;
   END LOOP;

   COMMIT;



   CLOSE LEN_CODE_COUNTRY;


   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'United States', ID_ = 'N'
    WHERE     REGEXP_LIKE (UPPER (NAME), '(USA|UNITED|ALASK)')
          AND SUBSTR (FK_NADR_MOD, 1, 1) = '1'
          AND ID_ = 'D';

   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Russia', ID_ = 'N'
    WHERE     REGEXP_LIKE (UPPER (NAME), '(RUSS|MOSCOW)')
          AND SUBSTR (FK_NADR_MOD, 1, 1) = '7'
          AND ID_ = 'D';

   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Canada', ID_ = 'N'
    WHERE     REGEXP_LIKE (UPPER (NAME), '(CANAD)')
          AND SUBSTR (FK_NADR_MOD, 1, 1) = '1'
          AND ID_ = 'D';

   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Kazakhstan', ID_ = 'N'
    WHERE     REGEXP_LIKE (UPPER (NAME), '(KAZA)')
          AND SUBSTR (FK_NADR_MOD, 1, 1) = '7'
          AND ID_ = 'D';

   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Inmarsat', ID_ = 'N'
    WHERE     REGEXP_LIKE (UPPER (NAME), '(INMAR)')
          AND SUBSTR (FK_NADR_MOD, 1, 1) = '8'
          AND ID_ = 'D';

   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'International Networks', ID_ = 'N'
    WHERE     REGEXP_LIKE (UPPER (NAME), '(INTERNATI)')
          AND SUBSTR (FK_NADR_MOD, 1, 1) = '8'
          AND ID_ = 'D';


   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Russia', ID_ = 'N'
    WHERE     REGEXP_LIKE (UPPER (NAME), '(OSETIJA|OSSETIA)')
          AND SUBSTR (FK_NADR_MOD, 1, 1) = '7'
          AND ID_ = 'D';


   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Satellite', ID_ = 'N'
    WHERE     REGEXP_LIKE (
                 UPPER (NAME),
                 '(GLOBALNET|ELLIP|EMNIFY|SIPME|VOXBON|SHARED|MEDIALINC|MALTAWINS)')
          AND SUBSTR (FK_NADR_MOD, 1, 2) = '88'
          AND ID_ = 'D';


   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Change route', ID_ = 'N'
    WHERE REGEXP_LIKE (UPPER (NAME), '(CHANGE ROUTE)') AND ID_ = 'D';



   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Freephone', ID_ = 'N'
    WHERE REGEXP_LIKE (UPPER (NAME), '(FREEPHONE)') AND ID_ = 'D';

   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'American Samoa', ID_ = 'N'
    WHERE     REGEXP_LIKE (UPPER (NAME), '(SAMOA)')
          AND SUBSTR (FK_NADR_MOD, 1, 3) = '684'
          AND ID_ = 'D';



   COMMIT;


   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Other Island', ID_ = 'N'
    WHERE REGEXP_LIKE (UPPER (NAME), '(ISLAND)') AND ID_ = 'D';

   COMMIT;

   UPDATE ALEX.R0108_RATE_SYS
      SET COUNTRY = 'Other', ID_ = 'N'
    WHERE ID_ = 'D';

   COMMIT;







   INSERT INTO ALEX.R0108_RATE_SYS_TMP (FK_NADR,
                                        FK_ORGA_OPER,
                                        FK_PROD,
                                        FK_TIER,
                                        NAME,
                                        FK_CURR,
                                        UNIT_COST,
                                        FK_RATE_FED,
                                        RATE_LED,
                                        FK_NADR_MOD,
                                        UNIT_COST_USD,
                                        UNIT_COST_EUR,
                                        COUNTRY,
                                        TYPE_NETW,
                                        ID_)
      SELECT FK_NADR,
             FK_ORGA_OPER,
             FK_PROD,
             FK_TIER,
             NAME,
             FK_CURR,
             UNIT_COST,
             FK_RATE_FED,
             RATE_LED,
             FK_NADR_MOD,
             UNIT_COST_USD,
             UNIT_COST_EUR,
             COUNTRY,
             TYPE_NETW,
             ID_
        FROM ALEX.R0108_RATE_SYS;

   COMMIT;

   EXECUTE IMMEDIATE 'truncate table ALEX.R0108_RATE_SYS';

   --UNIT TEST
   INSERT INTO ALEX.R0108_RATE_SYS_UNIT_TEST (SYSDATE_, NAME_STRING_, COUNT_)
      SELECT SYSDATE, 'COUNTRY_STR415', COUNT (*)
        FROM ALEX.R0108_RATE_SYS_TMP;

   COMMIT;

   --END TEST UNIT



   UPDATE ALEX.R0108_RATE_SYS_TMP
      SET TYPE_NETW = 'PRS'
    WHERE     TYPE_NETW IS NULL
          AND REGEXP_LIKE (
                 UPPER (REPLACE (NAME, ' ')),
                 '(PERSONAL|SPECIAL|PREMIUM|PAGING|VAS|UNIVERSAL|CORPORATE|MARITIME|AUDIOTEXT|MOBILITYSERVICES|ACCESSNUMBER|SHAREDCOST)')
          AND NOT REGEXP_LIKE (UPPER (REPLACE (NAME, ' ')),
                               '(CHUVASHI|SEVASTOP)');

   COMMIT;


   UPDATE ALEX.R0108_RATE_SYS_TMP
      SET TYPE_NETW = 'SAT'
    WHERE     TYPE_NETW IS NULL
          AND REGEXP_LIKE (
                 UPPER (REPLACE (NAME, ' ')),
                 '(GLOBALNETWORKS|INTERNATIONALNETWORK|SATELITE|SATELLITE|INMARSAT|IRIDIUM|GLOBALSTAR|THURAYA|DISASTER|SATELLITE|EMSAT|VOXBONE|ELLIPSO)')
          AND TYPE_NETW IS NULL;

   COMMIT;

   UPDATE ALEX.R0108_RATE_SYS_TMP
      SET TYPE_NETW = 'MOB'
    WHERE     TYPE_NETW IS NULL
          AND REGEXP_LIKE (UPPER (REPLACE (NAME, ' ')),
                           '(MOBILE|CELLULAR|MOB|ROAMING)')
          AND TYPE_NETW IS NULL;

   COMMIT;



   UPDATE ALEX.R0108_RATE_SYS_TMP
      SET TYPE_NETW = 'FIX'
    WHERE TYPE_NETW IS NULL;

   COMMIT;


   UPDATE ALEX.R0108_RATE_SYS_TMP
      SET TYPE_NETW = 'SAT'
    WHERE REGEXP_LIKE (UPPER (REPLACE (COUNTRY, ' ')),
                       '(SATELLITE|GLOBALSTAR|INTERNATIONALNETWORK)');

   COMMIT;



   --   UPDATE ALEX.R0108_RATE_SYS_TMP
   --      SET TYPE_NETW = 'PRS'
   --    WHERE REGEXP_LIKE (UPPER (REPLACE (COUNTRY, ' ')),
   --                       '(SPECIALSERVICES|PERSONAL)');
   --
   --   COMMIT;



   INSERT INTO ALEX.R0108_RATE_SYS_AVG_TYPE (FK_ORGA_OPER,
                                             AVG_UNIT_COST_USD,
                                             AVG_UNIT_COST_EUR,
                                             COUNTRY)
        SELECT FK_ORGA_OPER,
               ROUND (AVG (UNIT_COST_USD), 4),
               ROUND (AVG (UNIT_COST_EUR), 4),
               COUNTRY
          FROM ALEX.R0108_RATE_SYS_TMP
         WHERE TYPE_NETW IN ('MOB', 'FIX')
      GROUP BY FK_ORGA_OPER, COUNTRY;

   COMMIT;



   --UNIT TEST
   INSERT INTO ALEX.R0108_RATE_SYS_UNIT_TEST (SYSDATE_, NAME_STRING_, COUNT_)
      SELECT SYSDATE, 'AVG_RATE_STR498', COUNT (*) FROM ALEX.R0108_RATE_SYS;

   COMMIT;

   --END TEST UNIT



   -- ПРОСТАВЛЯЕМ СРЕДНЮЮ СТАВКУ НА НАПРАВЛЕНИЯ
   INSERT INTO ALEX.R0108_RATE_SYS (FK_NADR,
                                    FK_ORGA_OPER,
                                    FK_PROD,
                                    FK_TIER,
                                    NAME,
                                    FK_CURR,
                                    UNIT_COST,
                                    FK_RATE_FED,
                                    UNIT_COST_USD,
                                    COUNTRY,
                                    UNIT_COST_EUR,
                                    AVG_COUNTRY_USD,
                                    AVG_COUNTRY_EUR,
                                    TYPE_NETW)
      SELECT RSYS.FK_NADR,
             RSYS.FK_ORGA_OPER,
             RSYS.FK_PROD,
             RSYS.FK_TIER,
             RSYS.NAME,
             RSYS.FK_CURR,
             RSYS.UNIT_COST,
             RSYS.FK_RATE_FED,
             RSYS.UNIT_COST_USD,
             RSYS.COUNTRY,
             RSYS.UNIT_COST_EUR,
             AV.AVG_UNIT_COST_USD,
             AV.AVG_UNIT_COST_EUR,
             RSYS.TYPE_NETW
        FROM    ALEX.R0108_RATE_SYS_TMP RSYS
             LEFT JOIN
                ALEX.R0108_RATE_SYS_AVG_TYPE AV
             ON (    RSYS.FK_ORGA_OPER = AV.FK_ORGA_OPER
                 AND RSYS.COUNTRY = AV.COUNTRY);

   COMMIT;

   EXECUTE IMMEDIATE 'truncate table ALEX.R0108_RATE_SYS_TMP';



   --UNIT TEST
   INSERT INTO ALEX.R0108_RATE_SYS_UNIT_TEST (SYSDATE_, NAME_STRING_, COUNT_)
      SELECT SYSDATE, 'AVG_RATE_STR540', COUNT (*) FROM ALEX.R0108_RATE_SYS;

   COMMIT;

   --END TEST UNIT



   UPDATE ALEX.R0108_RATE_SYS
      SET FRAUD_TIER = 'PRS_TYPE'
    WHERE TYPE_NETW = 'PRS';


   COMMIT;


   --   Если ставка PRS или Fixed направления больше 0.25 EUR/USD и больше 20%
   --   средневзвешенной мобильной ставке по стране (AWR)
   --    ? направление подаётся на закрытие. - проставляем FRAUD_TIER = 'PRS'
   --
   UPDATE ALEX.R0108_RATE_SYS
      SET FRAUD_TIER = 'PRS_RATE'
    WHERE     TYPE_NETW IN ('FIX', 'PRS', 'MOB')
          AND AVG_COUNTRY_EUR IS NOT NULL
          AND UNIT_COST_EUR >= V_EUR_RATE_FR
          AND UNIT_COST_EUR >= (AVG_COUNTRY_EUR * V_EUR_AVG_PROC_FR);

   --AND AVG_COUNTRY_EUR >= (AVG_COUNTRY_EUR * V_EUR_AVG_PROC_FR);


   COMMIT;



   --   5.    Если ставка Mobile направления больше 0.70 EUR/USD ? направление подаётся
   --   на закрытие.(Закрытие дорогих Mobile сетей). - проставляем FRAUD_TIER = 'PRS'



   UPDATE ALEX.R0108_RATE_SYS
      SET FRAUD_TIER = 'PRS_RATE'
    WHERE     TYPE_NETW IN ('MOB', 'FIX', 'PRS')
          AND UNIT_COST_EUR >= V_EUR_MAX_RATE_FR;


   COMMIT;
   
   
   
      --УДАЛЕНИЕ ДАННЫХ, КОТОРЫЕ НЕ ВХОДЯТ В ТАБЛИЦУ МАРШРУТИЗАЦИИ
   --==========================================================================
   DELETE ALEX.R0108_RATE_SYS
    WHERE UPPER (FK_ORGA_OPER || COUNTRY) NOT IN
             (  SELECT UPPER (T_OPERATOR || T_COUNTRY)
                  FROM ALEX.R0108_ROUTE_IXTOOLS
              GROUP BY UPPER (T_OPERATOR || T_COUNTRY));


   COMMIT;


   --==========================================================================
   
   

   /*
   необходимо создать отдельную таблицук тиров и наполнить ее данніми по услувию FRAUD_TIER = 'PRS_RATE'
   если исходящий вызов будет на один из указанных в таблице тиров с одного номера более 2 соединений - добавлять в отчет
   */
--   EXECUTE IMMEDIATE 'truncate table ALEX.R0108_PRS_TIER';
--
--   INSERT INTO ALEX.R0108_PRS_TIER (FK_TIER, FRAUD_TIER, BIL_OP)
--        SELECT FK_TIER, FRAUD_TIER, FK_ORGA_OPER
--          FROM ALEX.R0108_RATE_SYS
--         WHERE FRAUD_TIER IN ('PRS_RATE', 'PRS_TYPE')
--      GROUP BY FK_TIER, FRAUD_TIER, FK_ORGA_OPER;
--
--   COMMIT;



   --UNIT TEST
--   INSERT INTO ALEX.R0108_RATE_SYS_UNIT_TEST (SYSDATE_, NAME_STRING_, COUNT_)
--      SELECT SYSDATE, 'PRS_TIER_STR603', COUNT (*) FROM ALEX.R0108_PRS_TIER;

   COMMIT;

   --END TEST UNIT

   --=============================================================================


   UPDATE alex.t_log_ex_proc
      SET EX_END_DATE = SYSDATE, id_ = 'new', CLEAN = 'N'
    WHERE id_ = 'new_tmp' AND NAME_PROC = 'R0108_RATE_TEX_SYS';

   COMMIT;
END R0108_RATE_TEX_SYS_TEST;
/