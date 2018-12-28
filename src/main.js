function isAdminUser() {
  return true;
}

function getAuthType() {
  var response = { type: 'NONE' };
  return response;
}

function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config.newInfo()
    .setId('instructions')
    .setText('TODO');

  config.newTextInput()
    .setId('awsAccessKeyId')
    .setName('AWS_ACCESS_KEY_ID');

  config.newTextInput()
    .setId('awsSecretAccessKey')
    .setName('AWS_SECRET_ACCESS_KEY');

  config.newTextInput()
    .setId('awsRegion')
    .setName('AWS Region of the Table')
    .setPlaceholder('us-east-1');

  config.newTextInput()
    .setId('databaseName')
    .setName('Glue Database')
    .setPlaceholder('default');

  config.newTextInput()
    .setId('tableName')
    .setName('Glue Table Name');

  config.newTextInput()
    .setId('outputLocation')
    .setName('Query Output Location')
    .setHelpText('S3 path to store the Athena query results')
    .setPlaceholder('s3://<bucket>/<directory>');

  config.newTextInput()
    .setId('rowLimit')
    .setName('Row Limit')
    .setHelpText('Max. number of rows to fetch in query. If set to empty or 0, all rows will be fetched.')
    .setPlaceholder('1000')
    .setAllowOverride(true);

  return config.build();
}

function getSchema(request) {
  var fields = getFieldsFromGlue(request).build();
  return { schema: fields };
}

function getData(request) {
  var data = getDataFromAthena(request);
  return data;
}
