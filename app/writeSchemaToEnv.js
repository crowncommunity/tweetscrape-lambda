const fs = require('fs');
const schema = JSON.parse(fs.readFileSync(__dirname + '/awsconfiguration.json'))

// console.log(schema);

process.stdout.write('AWS_AUTH_REGION=' + schema.CognitoUserPool.Default.Region + '\n')
process.stdout.write('AWS_AUTH_POOL=' + schema.CognitoUserPool.Default.PoolId + '\n')
process.stdout.write('AWS_AUTH_CLIENT=' + schema.CognitoUserPool.Default.AppClientId + '\n')
process.stdout.write('AWS_API_ENDPOINT=' + schema.APIGateway.ApiGatewayRestApi.Endpoint + '\n')
process.stdout.write('AWS_API_REGION=' + schema.APIGateway.ApiGatewayRestApi.Region + '\n')
