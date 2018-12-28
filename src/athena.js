function generateAthenaQuery(request) {
  var columns = request.fields.map(function (field) {
    return '"' + field.name + '"';
  });
  var table = request.configParams.tableName;
  var rowLimit = parseInt(request.configParams.rowLimit);

  var query = 'SELECT ' + columns.join(', ') + ' FROM "' + table + '"';
  if (rowLimit) {
    query += ' LIMIT ' + rowLimit;
  }

  return query;
}

function runAthenaQuery(region, database, query, outputLocation) {
  var payload = {
    'ClientRequestToken': uuidv4(),
    'QueryExecutionContext': {
      'Database': database
    },
    'QueryString': query,
    'ResultConfiguration': {
      'OutputLocation': outputLocation
    }
  };
  return AWS.post('athena', region, 'AmazonAthena.StartQueryExecution', payload);
}

function waitAthenaQuery(region, queryExecutionId) {
  var payload = {
    'QueryExecutionId': queryExecutionId
  };

  // Ping for status until the query reached a terminal state
  while (1) {
    var result = AWS.post('athena', region, 'AmazonAthena.GetQueryExecution', payload);
    var state = result.QueryExecution.Status.State.toLowerCase();
    Logger.log(state);
    switch (state) {
      case 'succeeded':
        return true;
      case 'failed':
        throw new Error(result.QueryExecution.Status.StateChangeReason || 'Unknown query error');
      case 'cancelled':
        throw new Error('Query cancelled');
    }

    Utilities.sleep(3000);
  }
}

function getAthenaQueryResults(region, queryExecutionId) {
  // Get results until all rows are fetched
  var rows = [];
  var nextToken = null;
  while (1) {
    var payload = {
      'QueryExecutionId': queryExecutionId
    };
    if (nextToken) {
      payload.NextToken = nextToken;
    }

    var result = AWS.post('athena', region, 'AmazonAthena.GetQueryResults', payload);
    var newRows = result.ResultSet.Rows.map(function (row) {
      return row.Data.map(function (data) {
        return data.VarCharValue;
      });
    });
    rows = rows.concat(newRows);

    nextToken = result.NextToken;
    if (!nextToken) {
      break;
    }
  }

  // Remove header row
  rows.shift();
  return rows;
}
