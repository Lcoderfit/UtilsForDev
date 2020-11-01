# -*- coding: utf-8 -*-
import os
import sys
from dimension_settings import dimension

table_info = u"""
presto:db_ods_test> desc db_dws_test.dws_ei_basic_gongshang_company_base_info_ds;
      Column       |   Type    |     Extra     |     Comment
-------------------+-----------+---------------+------------------
 craw_id           | varchar   |               | 抓取id
 id                | varchar   |               | 公司id
 org_name          | varchar   |               | 公司名称
 legal_person_name | varchar   |               | 法人姓名
 reg_number        | varchar   |               | 注册号
 company_org_type  | varchar   |               | 公司类型
 reg_location      | varchar   |               | 注册地址
 estiblish_time    | timestamp |               | 成立日期
 business_term     | varchar   |               | 营业期限
 business_scope    | varchar   |               | 经营范围
 reg_institute     | varchar   |               | 登记机关
 approved_time     | timestamp |               | 核准时间
 reg_status        | varchar   |               | 企业状态
 reg_capital       | varchar   |               | 注册资金(数值)
 reg_capital_cn    | varchar   |               | 注册资金(万元)
 currency_unit     | varchar   |               | 货币单位
 uscc              | varchar   |               | 统一社会信用代码
 industry_code     | varchar   |               | 行业代码
 reg_org_code      | varchar   |               | 行政区划代码
 province          | varchar   |               | 省
 city              | varchar   |               | 市
 district          | varchar   |               | 区
 longitude         | varchar   |               | 经度
 latitude          | varchar   |               | 纬度
 his_name          | varchar   |               | 曾用名
 source            | varchar   |               | 数据来源
 crawledtime       | timestamp |               | 抓取时间
 institution_code  | varchar   |               | 组织机构代码
 email             | varchar   |               | email
 tel               | varchar   |               | 电话
 url               | varchar   |               | 网址
 busi_status_code  | varchar   |               | 经营状态代码
 company_type_code | varchar   |               | 企业性质代码
 org_code          | varchar   |               | org_code
 valid             | tinyint   |               | valid
 p_date            | varchar   | partition key | 日分区
(36 rows)

Query 20201030_084320_01570_ytj5w, FINISHED, 4 nodes
Splits: 70 total, 70 done (100.00%)
0:00 [36 rows, 4.27KB] [184 rows/s, 21.8KB/s]

"""

# 目前暂时添加以下四种类型，后续遇到其他类型再添加
type_dic = {
    u"date": u"DateType()",
    u"timestamp": u"TimestampType()",
    u"tinyint": u"IntegerType()",
    u"varchar": u"StringType()",
}


def desc_to_schema(desc_info):
    """根据在presto中使用desc命令输出的表信息生成对应的spark schema"""
    res_str = u"schema = StructType([\n"
    lines = iter(desc_info.split("\n"))
    for line in lines:
        col_list = line.split("|")
        # 列名那一行和p_date那一行不处理
        if (len(col_list) <= 1) or (len(col_list[2].strip()) > 0):
            continue
        field_name, field_type = col_list[0], col_list[1]
        row_str = u"    StructField(\"" + field_name.strip() + "\", " + type_dic[field_type.strip()] + ", True),\n"
        res_str += row_str

    return res_str + u"])"


def desc_to_sql(desc_info, suffix):
    """根据在presto中使用desc命令输出的表信息生成对应的sql字段"""
    res_str = u""
    lines = iter(desc_info.split("\n"))
    for line in lines:
        col_list = line.split("|")
        # 列名那一行和p_date那一行不处理
        if (len(col_list) <= 1) or (len(col_list[2].strip()) > 0):
            continue
        field_name, field_type = col_list[0], col_list[1]
        row_str = u" " * 16 + suffix + field_name.strip() + u",\n"
        res_str += row_str

    return res_str


if __name__ == "__main__":
    table_name = ""
    desc_info_cur = ""
    query_sql = "echo 'desc {};'"
    # db_shell = "~/workspace/luhu/p.sh"
    db_shell = "/home/a.sh"

    params = sys.argv
    if len(params) < 2:
        raise Exception("params is less than 2")
    mode = params[1]
    if not mode.isdigit():
        raise Exception("params is invalid, must be integer type")

    # 模式1自输入表名，模式2采用默认表名模式
    # python desc_to_schema.py 1 db_dwd_test.dwd_ei_basic_tsc_lawsuit_detail_ds
    if mode == "1":
        if len(params) < 3:
            raise Exception("mode 1, customize table name, params can't less than 3")
        table_name = params[2]
    elif mode == "2":
        if len(params) < 3:
            raise Exception("mode 2, params can't less than 3")
        dimension_index = params[2]
        if dimension_index not in dimension:
            raise Exception("mode 2, input dmension can't find in dimension settings")
        dimension_cur = dimension.get(dimension_index)
        table_name = "db_dwd.dwd_ei_basic_gongshang_{}_ds".format(dimension_cur)
    else:
        raise Exception("mode param error")

    desc_info_cur = "".join(os.popen("{}|{}".format(query_sql.format(table_name), db_shell)).readlines())
    print(desc_to_schema(desc_info_cur))
    print(desc_to_sql(desc_info_cur, "a."))
