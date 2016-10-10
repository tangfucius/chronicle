# chronicle

A Flask app that provides the following features:
- Use a config file to aggregate raw data from AWS Redshift to Elasticsearch daily
- Create charts/dashboards from the Elasticsearch datastore using your own equations and time frequency
- Add annotations to Elasticsearch and display the relevant ones on the charts (clicking on the annotations will pop up the details)
- Make SQL queries of AWS Redshift on the browser. The results can be pivoted, and line charts are automatically created if it is a time series.
