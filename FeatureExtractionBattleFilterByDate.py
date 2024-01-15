from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType
import sys



def trans_date(date_str):
    return date_str[0:4]+"-"+date_str[4:6]+"-"+date_str[6:8]

if __name__ == '__main__':

    start_date = sys.argv[1]
    end_date = sys.argv[2]
    hero_id = sys.argv[3]
    team_elo = sys.argv[4]
    # 108,117,118,119,121,122,127,136
    modetids = sys.argv[5]
    sdk_version = sys.argv[6]
    score = sys.argv[7]

    conf = {'mapreduce.output.fileoutputformat.compress': 'false'
           }

    spark = SparkSession \
        .builder \
        .appName("spark_sql2 example") \
        .getOrCreate()

    # 创建三个原始视图
    player_battle_df = spark.read.json("hdfs:///staging/shuguang/server/100055/PlayerBattle/*/*/*/*")
    print("---- playerbattle_schema: ")
    player_battle_df.printSchema()
    player_battle_df.registerTempTable("shuguang_dwd_playerbattle_d_view")

    battle_df = spark.read.json("hdfs:///staging/shuguang/server/100055/Battle/*/*/*/*")
    print("---- battle_schema: ")
    battle_df.printSchema()
    battle_df.registerTempTable("shuguang_dwd_battle_d")

    matching_ensure_df = spark.read.json("hdfs:///staging/shuguang/server/100055/MatchingEnsure/*/*/*/*")
    print("---- matchingensure_schema: ")
    matching_ensure_df.printSchema()
    matching_ensure_df.registerTempTable("shuguang_dwd_matchingensure_d")

    # 构建battle全量视图
    # battle表和playerbattle表的并集，keyversion、faction_1_total_kill、faction_2_total_kill、faction_total_kill来自playerbattle表的加工
    # collect_list(cast(gametype as string))[0] 等价于 coalesce(split_part(group_concat(cast(gametype as string),','),',',1))
    sql1 = """
    select 
    u.battleid,
    collect_list(cast(u.pt_dt as string))[0] as pt_dt,
    collect_list(cast(keyversion as string))[0] as keyversion,
    collect_list(cast(faction_1_total_kill as string))[0] as faction_1_total_kill,
    collect_list(cast(faction_2_total_kill as string))[0] as faction_2_total_kill,
    collect_list(cast(faction_total_kill as string))[0] as faction_total_kill
    from (
    select 
    battleid,
    null as pt_dt,
    null as keyversion, null as faction_1_total_kill, null as faction_2_total_kill, null as faction_total_kill
    from shuguang_dwd_battle_d 
    union all
    select
    battleid,
    collect_list(cast(substr(logtime,1,10) as string))[0] as pt_dt,
    collect_list(if(keyversion is null or keyversion='' or length(trim(keyversion))=0 or trim(keyversion)='null' or trim(keyversion)='Null' or trim(keyversion)='NULL', null, keyversion))[0] as keyversion,
    sum(if(factionid=1, kill, 0)) as faction_1_total_kill, sum(if(factionid=2, kill, 0)) as faction_2_total_kill, sum(kill) as faction_total_kill
    from shuguang_dwd_playerbattle_d_view group by battleid
    ) u group by u.battleid
    """

    shuguang_dwd_battle_d_view_df = spark.sql(sql1)
    print("---- shuguang_dwd_battle_d_view_schema: ")
    shuguang_dwd_battle_d_view_df.printSchema()
    print("---- shuguang_dwd_battle_d_view_sql: "+str(sql1))
    shuguang_dwd_battle_d_view_df.registerTempTable("shuguang_dwd_battle_d_view")

    # 曙光AI数据过滤视图，方便sql一体化
    sql2 = """
    SELECT DISTINCT
    tba.pt_dt,substr(tba.pt_dt,1,7) as pt_dt_date,tba.battleid,tbc.keyversion,tbc.faction_1_total_kill,tbc.faction_2_total_kill,tbc.faction_total_kill
    FROM
    (
        SELECT 
        substr(logtime,1,10) as pt_dt,
        role_id,
        battleid
        FROM shuguang_dwd_playerbattle_d_view 
        WHERE 
        battleid is not null
        and substr(logtime,1,10) >= '{start_date}'
        and substr(logtime,1,10) <='{end_date}'
        and usernum=10
        and modetid in ({modetids})
        and herotid = {hero_id}
        and score >={score}
    ) tba
    INNER JOIN
    (   
        SELECT
        matching_id,
        role_id,
        team_elo
        FROM shuguang_dwd_matchingensure_d 
        WHERE 1=1
        and team_elo>={team_elo}
    ) tbb 
    on tba.battleid = tbb.matching_id 
    and tba.role_id = tbb.role_id
    inner JOIN 
    (
        select battleid,keyversion,faction_1_total_kill,faction_2_total_kill,faction_total_kill
        from shuguang_dwd_battle_d_view
        WHERE 1=1
        and pt_dt >= '{start_date}'
        and pt_dt <='{end_date}'
    ) tbc
    on tba.battleid = tbc.battleid
    """.format(start_date=trans_date(start_date),end_date=trans_date(end_date),hero_id=hero_id,team_elo=team_elo,modetids=modetids,sdk_version=sdk_version,score=score)

    shuguang_ai_battle_info_filter_df = spark.sql(sql2)
    print("---- shuguang_ai_battle_info_filter_schema: ")
    shuguang_ai_battle_info_filter_df.printSchema()
    print("---- shuguang_ai_battle_info_filter_sql: " + str(sql2))
    shuguang_ai_battle_info_filter_df.registerTempTable("shuguang_ai_battle_info_filter")

    # keyversion filter
    sql3 = """
    select pt_dt,pt_dt_date,battleid,keyversion,faction_1_total_kill,faction_2_total_kill,faction_total_kill
    from shuguang_ai_battle_info_filter where pt_dt < '2021-02-01' and keyversion in ("7e2f822dbce0a467fa2ff4d73d15ba79","fd60727790f071afcdbef957398facfb","5e19e13dfc75f4c189af19824ef19f60","3a3c6195b5b2f83a725e8643d89dd8cd","93bc2e52028862e4c52a769890789897","0a37bdd2907051b125fea063070ccf32","2fe302084292bac40498494db90d1b5a","403ce5730541409eaf94629b0f5e2392","9e1e37d233343d3605c7e8ecbb43c337","0abddf9ca85150b48f4d47ca1e4babb2","1ccc50f7d9c2ee0f25484a39480c3a22","f094aca2ce15aa18bf4d4445167a1e05","9f361b1072313ee5703a94f0e17c9f59","7f510e9f9a64d00c3bdbe16f7935d950","e2535a5f10f391a2d80edf6f1f79209c","a2a1be1a319237a3b49bc35425060d47","9bd9f3ff714ff988a216cc2866ac51bf","d7e8dbbf1b999c1aeec23064222ac492","e2a76a934e6899542942d3f2528d9084","84d2e38efdeaed20aab8d614d1ec83ff","5e746f1129aea092de5f7a1d6984c434","85011653d817dc4f549d2c32762ab4b6","ca5ad93519bec1a9b213773826716620","41219986359fdf9b4c5f904041d88d65","2bbc312aaceb5ca64c7b7ef5a0db0333","42c79a0b758715bc66147e3083d4815b","375fabc77a4c2c206005b22815416786","28dabaf4731ea0a5c7b024d6ce71dcfa","a5767d7203ae99717acc705ab0293954","409326c188b8d8e760495d381697e1bd","0e85bee7b54622d0bfc9898d55a62949","e6845e4a2bdc64a4cc6c61681dd65166","e103bac6acdc0059fa780613c3de4262","da1859457d5b76cb3f9a269f2f8aeb6d","bdd04d59f24f71621c19f1625a30fe2a","dfdaf605cfe13e50cae1d1b41a48f771","2152b6b44c2eaab4f60d9cad3c56ed5d","5d4157de0ed41161385c38d18443d5a3","dc0ac9a1390d4dd66925d3bf474521a0","20162a1aaa33a9ee581ce9f317d65087","7c98a5c9f90adad30ab21f2b1e667f65","d353ce2f89a84766c6225cb687c48167","1b2c402dac38001174f05d25a9dc3d4a","9c32be41a504980a0e27428944c7d371","d9931a841d25f89b5ccf04ca0225f7de","3ec352627bcad30a46e75ea6a8c36a83","3801b5326fbcbb4cdeeb409b4b95f160","97426cd502521b192843d32bc186eaf3","a47804534a4ab6345218232b21c269fa","15c13005aace489d540eec3c0078d419","b8e95d078a4c9b290ef5603e276c333f","aca63a6b28e99dd99f58adcf8b0fd727","53ec1201c485f9f5b902ea9ca60d6e26","188a3d88f06b778e276e52455719a2cd","b272bacc55813a44f35d95b87ae04b0d","8c87800eb9b0e96c31e368e6b0665bbe","a14c0db1a1c9c14250d8955b64c50bd8","ce9914d779bb76515f3e5c53c1b65d78","f5ffa2073fd97ccd7148a61e35502a3f","a044723084a4041aee1e77fff8b70c23","7db6a2c60b8f3b2cb436efe64056a132","4ffc29e19b6699dfc2d7e06029164e14","89932334c7131bf2437d49b6d6cc9e8f","2dc9d1b9f24c60b77eac1c2a605530c3","b5e0a23bae331bf2c35877f61d9a1c53","4f7981ae15f7cf18b332593a0be0ef67","62ce46d2b3909162190f127d7da7fd9d","3c85d48078eae722f0936fb1099061b2","5e3c73d1c3d2fd1dd7308b84ce877f97","baf4ad3d518b8fee6515573b56104823","c578a4e8cb33a761be4bf0c3f66fb05f","645b0be4913dab734b94830038e10f07","bc80257e2e51b4712771b3175ad369cf","e3d5a94a0e769cd98c13fca72e812388","8d31d433401ceead9b2efec608520f67","6c9de2090002b84b1af65e528eed6819","e8b4776f68403a70141f80571e1c8224","8c9945acabcc9416b9ab523cdb6d2523","b335f93828c3260886266237397b3c85","4274deafa9c73eda59d89d68fc038216")
    union all
    select pt_dt,pt_dt_date,battleid,keyversion,faction_1_total_kill,faction_2_total_kill,faction_total_kill
    from shuguang_ai_battle_info_filter where pt_dt >= '2021-02-01'
    """

    shuguang_ai_battle_info_keyversion_filter_df = spark.sql(sql3)
    print("---- shuguang_ai_battle_info_keyversion_filter_schema: ")
    shuguang_ai_battle_info_keyversion_filter_df.printSchema()
    print("---- shuguang_ai_battle_info_keyversion_filter_sql: " + str(sql3))
    shuguang_ai_battle_info_keyversion_filter_df.registerTempTable("shuguang_ai_battle_info_keyversion_filter")

    # 测试月筛选对局数量
    # sqltest2 = """
    # select
    # e.pt_dt_date, count(*)
    # from shuguang_ai_battle_info_keyversion_filter e group by e.pt_dt_date
    # """
    # test2_df = spark.sql(sqltest2)
    # test2_df.show()

    # 查询对局最后结果
    sql4 = """
    select
    e.pt_dt, concat_ws('\001',
    if(e.battleid is not null,cast(e.battleid as string),''),
    if(e.keyversion is not null,cast(e.keyversion as string),''),
    if(e.faction_1_total_kill is not null,cast(e.faction_1_total_kill as string),''),
    if(e.faction_2_total_kill is not null,cast(e.faction_2_total_kill as string),''),
    if(e.faction_total_kill is not null,cast(e.faction_total_kill as string),'')) as battle_info
    from shuguang_ai_battle_info_keyversion_filter e
    """

    battle_result_df = spark.sql(sql4)
    print("---- battle_result_sql: " + str(sql4))

    battle_result_df.rdd.map(lambda p: (p.pt_dt, p.battle_info)) \
        .groupByKey().flatMapValues(lambda i: i) \
        .saveAsHadoopFile(
        path="hdfs:///staging/shuguang/rec_parse/feature_extraction/battle_info/" + hero_id + "/" + sdk_version + "/" + start_date.replace('-','') + "-" + end_date.replace('-','') + "/",
        keyClass="java.lang.String",
        valueClass="java.lang.String",
        outputFormatClass="cn.jj.simulation.utils.newHadoopApi.output.RDDMultipleTextOutputFormat", conf=conf)

    # 查询玩家最后结果
    sql5 = """
    select
    b.pt_dt,concat_ws('\001',
        if(b.battleid is not null,cast(b.battleid as string),''),
        if(e.role_id is not null,cast(e.role_id as string),''),
        if(e.gold is not null,cast(e.gold as string),''),
        if(e.score is not null,cast(e.score as string),''),
        if(e.kill is not null,cast(e.kill as string),''),
        if(e.death is not null,cast(e.death as string),''),
        if(e.assist is not null,cast(e.assist as string),''),
        if(e.rank_tid is not null,cast(e.rank_tid as string),''),
        if(e.continuekill is not null,cast(e.continuekill as string),''),
        if(e.towerkill is not null,cast(e.towerkill as string),''),
        if(e.creepkill is not null,cast(e.creepkill as string),''),
        if(e.dmgtohero is not null,cast(e.dmgtohero as string),''),
        if(e.dmgtotower is not null,cast(e.dmgtotower as string),''),
        if(e.dmgreceived is not null,cast(e.dmgreceived as string),''),
        if(e.magdmgreceived is not null,cast(e.magdmgreceived as string),''),
        if(e.doublekill is not null,cast(e.doublekill as string),''),
        if(e.triblekill is not null,cast(e.triblekill as string),''),
        if(e.quadrakill is not null,cast(e.quadrakill as string),''),
        if(e.pentakill is not null,cast(e.pentakill as string),''),
        if(e.healingtohero is not null,cast(e.healingtohero as string),''),
        if(e.herotid is not null,cast(e.herotid as string),''),
        if(e.modetid is not null,cast(e.modetid as string),''),
        if(e.usernum is not null,cast(e.usernum as string),''),
        if(m.team_elo is not null,cast(m.team_elo as string),'')) as battle_info
    from shuguang_ai_battle_info_keyversion_filter b
    left join (
        select battleid,role_id,gold,score,kill,death,assist,rank_tid,'' as continuekill,towerkill,creepkill,dmgtohero,dmgtotower,dmgreceived,magdmgreceived,
            '' as doublekill,triblekill,quadrakill,pentakill,healingtohero,herotid,modetid,usernum
        from shuguang_dwd_playerbattle_d_view
        where 1=1
        and substr(logtime,1,10) >= '{start_date}'
        and substr(logtime,1,10) <='{end_date}'
    ) e
    on b.battleid = e.battleid
    left join (
        SELECT
        matching_id,
        role_id,
        team_elo
        FROM shuguang_dwd_matchingensure_d 
        WHERE 1=1
        and substr(logtime,1,10) >= '{start_date}'
        and substr(logtime,1,10) <='{end_date}'
    ) m
    on e.battleid = m.matching_id
    and e.role_id = m.role_id
    """.format(start_date=trans_date(start_date),end_date=trans_date(end_date))
    player_result_df = spark.sql(sql5)
    print("---- player_result_sql: " + str(sql5))

    player_result_df.rdd.map(lambda p: (p.pt_dt, p.battle_info))\
        .groupByKey().flatMapValues(lambda i:i)\
        .saveAsHadoopFile(path="hdfs:///staging/shuguang/rec_parse/feature_extraction/player_info/"+hero_id+"/"+sdk_version+"/"+start_date.replace('-','')+"-"+end_date.replace('-','')+"/",
                          keyClass="java.lang.String",
                          valueClass="java.lang.String",
                          outputFormatClass="cn.jj.simulation.utils.newHadoopApi.output.RDDMultipleTextOutputFormat",conf=conf)