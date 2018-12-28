Run _dataflow_ job:
```
mvn compile exec:java -Dexec.mainClass=className \
 -Dexec.args="--runner=DataflowRunner --project=projectID \
  --input=INPUT_PATH --output=OUTPUT_PATH \
  --zone=europe-west1-b --region=europe-west1 \
  --numWorkers=16 --maxNumWorkers=32" -Pdataflow-runner
```

