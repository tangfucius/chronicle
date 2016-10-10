# chronicle

A Flask app (Vue.js is the frontend framework) that provides the following features:
- Use a config file to aggregate raw data from AWS Redshift to Elasticsearch daily
- Create charts/dashboards from the Elasticsearch datastore using your own equations and time frequency (plot.ly js is used for charting)
- Add annotations to Elasticsearch and display the relevant ones on the charts (clicking on the annotations will pop up the details)
- Make SQL queries of AWS Redshift on the browser. The results can be pivoted, and line charts are automatically created if it is a time series.

Used at Funplus to let teams create their own annotated charts, so that different teams can share information easily and visualize how certain events affect metrics (e.g. when a game has an update or when an ad campaign began).

The backend of features are implemented in views.py and helpers.py.
