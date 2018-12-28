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
    .setName('AWS Region')
    .setPlaceholder('us-east-1');

  config.newTextInput()
    .setId('databaseName')
    .setName('Glue Database Name')
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
    .setId('dateRangeColumn')
    .setName('Date Range Column Name')
    .setHelpText('(Optional) If date range is applied in the report, the corresponding column to apply the filtering conditions.');

  config.newTextInput()
    .setId('rowLimit')
    .setName('Row Limit')
    .setHelpText('Maximum number of rows to fetch in each query. Default is 1000. If set to -1, all rows will be fetched.')
    .setPlaceholder('1000')
    .setAllowOverride(true);

  config.setDateRangeRequired(true);

  return config.build();
}

function throwUserError(message) {
  DataStudioApp.createCommunityConnector()
    .newUserError()
    .setText(message)
    .throwException();
}

function validateConfig(configParams) {
  configParams = configParams || {};
  if (!configParams.awsAccessKeyId) {
    throwUserError('AWS_ACCESS_KEY_ID is empty.');
  }
  if (!configParams.awsSecretAccessKey) {
    throwUserError('AWS_SECRET_ACCESS_KEY is empty.');
  }
  if (!configParams.awsRegion) {
    throwUserError('AWS Region is empty.');
  }
  if (!configParams.databaseName) {
    throwUserError('Database Name is empty.');
  }
  if (!configParams.tableName) {
    throwUserError('Table Name is empty.');
  }
  if (configParams.outputLocation.indexOf('s3://') !== 0) {
    throwUserError('Query Output Location must in the format of s3://<bucket>/<directory>');
  }
  if (configParams.rowLimit) {
    var rowLimit = parseInt(configParams.rowLimit);
    if (isNaN(rowLimit)) {
      throwUserError('Invalid Row Limit.');
    }
  }
}

function getSchema(request) {
  validateConfig(request.configParams);
  try {
    var fields = getFieldsFromGlue(request).build();
    return { schema: fields };
  } catch (err) {
    throwUserError(err.message);
  }
}

function getData(request) {
  validateConfig(request.configParams);
  try {
    var data = getDataFromAthena(request);
    return data;
  } catch (err) {
    throwUserError(err.message);
  }
}
