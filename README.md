## Nsdi Crawler 및 DB 적재 자동화

### 프로젝트 소개
|구분|내용|
|------|---|
|한줄 소개|국가공간정보포털에서 토지이용계획정보, 토지특성정보를 크롤링하여 S3에 저장 후 저장된 데이터를 DB에 자동으로 적재합니다.|
|진행 기간|2020.09 ~ 2020.10|
|주요 기술| Python, AWS S3, AWS CloudWatch, AWS ECR, Docker, PostgreSQL|
|팀원 구성|개인 프로젝트|
|전담 역할|Crawler 구현 및 DB 적재 자동화|
|수상|없음|

### 프로젝트 개요
- 해당 프로젝트의 경우 데이터를 수집하여 S3에 업로드하는 Crawler와 S3에 저장된 데이터를 DB에 저장하는 Store로 구성되어 있습니다.
- Crawler의 경우 한달 간격으로 [토지이용계획정보](http://openapi.nsdi.go.kr/nsdi/eios/ServiceDetail.do?svcSe=F&svcId=F014)와 [토지특성정보](http://openapi.nsdi.go.kr/nsdi/eios/ServiceDetail.do?svcSe=F&svcId=F024) 에서 CSV 파일을 S3에 업로드합니다.
- Store의 경우 S3에 업로드 된 CSV 파일을 파싱하여 DB에 Bulk Load 합니다.
- 배포를 위해 docker-compose 파일을 구성하였으며 develop 브랜치에 병합시에 ECR에 자동으로 배포되도록 git action 파일을 추가하였습니다.

### 프로젝트 사용 기술 및 라이브러리


### ✔ Languauge

- Python

### ✔ Data Base

- PostgreSQL

### ✔ Dependency Management

- Poetry

### ✔ 협업

- Github

### ✔ Infra

- Docker
- AWS S3
- AWS CloudWatch
- AWS ECR
- Git Action

### ✔ Library

- BeautifulSoup
- SqlAlchemy
- Boto3 등


### [🛠 자세한 설명]

[노션 문서](https://www.notion.so/Nsdi-Crawler-DB-b672447b6d274841895b9f32a5286eef)
