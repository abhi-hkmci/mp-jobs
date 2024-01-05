# mp-jobs


# how to test 

1. set env variable as below 



## how to deploy 

gcloud run jobs deploy stage-maintanance \
    --source ./mp-job1 \
    --tasks 1 \
    --set-env-vars DATABASE_URL="postgres://postgres:hkmci23get4@35.220.189.93:5432/get" \
    --max-retries 2 \
    --region asia-east2 \
    --project=masterplanner


## how to run 

gcloud run jobs execute mp-job1 --region asia-east2