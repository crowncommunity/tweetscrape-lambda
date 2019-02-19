const AWS = require( 'aws-sdk' );

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

module.exports.logevent = async ( event, context ) => {
    console.log(event)
}


module.exports.hello = ( event, context, callback ) => {
	console.log( 'event', event );
    // console.log( 'context', context);
    console.log('env', process.env)

	const response = {
		statusCode: 200,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify( {
			"message": "World!"
		} ),
	}

	callback( null, response );
}
