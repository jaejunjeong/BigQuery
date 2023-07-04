## Python API를 활용한 BQ 사용(사용 전, Google Cloud Service Credential ID 및 KEY 필요)
# Library Import
from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery Client 생성 (Service Key File 사용)
project_id = 'temp-project'
key_path = "./key.json"

credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=project_id)

org = client.query("select 1").to_dataframe()
print(org)

## Google Colab를 활용한 BQ 사용(사용 전, Google Cloud Service Credential ID 및 KEY 필요)
# Library Import 및 구글 계정 인증
from google.colab import auth
auth.authenticate_user()

# Magic Command
%%bigquery --project minwoo-lee org

select ...

# Pandas.read_gbq
import pandas as pd

project_id = 'temp-project'
org = pd.read_gbq(query, project_id=project_id)
print(org)

## EDA
# pandas profiling 설치
!pip install https://github.com/pandas-profiling/pandas-profiling/archive/master.zip

# 패키지 임포트
import numpy as np 
import pandas as pd 
from pandas_profiling import ProfileReport

# 원하는 데이터 추출
WITH data AS (
    SELECT
        DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS date, 
        event_name,
        user_pseudo_id,
        platform,
        geo.continent,
        traffic_source.source,
        MOD(ABS(FARM_FINGERPRINT(CAST(user_pseudo_id AS STRING) || "_aatest_1")), 2) AS assignment
    FROM 
        `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
),
visit_users AS (
    SELECT 
        date, 
        user_pseudo_id,
        assignment,
        platform,
        continent,
        source,
    FROM 
        data
    GROUP BY 1, 2, 3, 4, 5, 6
),
purchase_users AS (
    SELECT 
        date, 
        user_pseudo_id
    FROM 
        data
    WHERE
        event_name = 'purchase'
    GROUP BY 1, 2
),
join_table as (
    SELECT
        v.*,
        cast(if(p.user_pseudo_id is null, FALSE, TRUE) as int64) AS conversion
    FROM 
        visit_users AS v
        LEFT JOIN purchase_users p using(date, user_pseudo_id)
)
SELECT * FROM join_table

# EDA 보고서 출력
profile = ProfileReport(df, title='california housing', html={'style': {'full_width': True}})
profile.to_notebook_iframe()

## aa test, t- test (equal_var : true일 경우 두 집단이 동일한 분산임을 가정)
from scipy import stats

test = stats.ttest_ind(
    df.loc[df['assignment'] == 1]['conversion'], 
    df.loc[df['assignment'] == 0]['conversion'], 
    equal_var=False)

print('t statistics : ', test.statistic)
print('p value : ', test.pvalue)

## feature analysis
import graphviz
from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import export_graphviz
from graphviz import Source
  
#prepare the df for the model by creating dummy vars and removing the label
df_dummy = pd.get_dummies(df)
train_cols = df_dummy.drop('conversion', axis=1)
  
#build the tree
tree=DecisionTreeClassifier(
    #set max tree dept at 4. Bigger than that it just becomes too messy
    max_depth=4,
    #change weights given that we have unbalanced classes. Our df set is now perfectly balanced. It makes easier to look at tree output
    class_weight="balanced",
    #only split if it's worthwhile. The default value of 0 means always split no matter what if you can increase overall performance, which might lead to irrelevant splits
    min_impurity_decrease = 0.001
    )
tree.fit(train_cols, df_dummy['conversion'])
  
#visualize it
export_graphviz(tree, out_file="tree.dot", feature_names=train_cols.columns, proportion=True, rotate=True)
s = Source.from_file("tree.dot")
s
