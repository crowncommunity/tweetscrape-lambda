module.exports.main = ( event, context, callback ) => {
	// console.log( 'event', event );
    // console.log( 'context', context);
    console.log('env', process.env)

	const response = {
		statusCode: 200,
		body: JSON.stringify( {
			"message": "World!"
		} ),
	}

	callback( null, response );
}
