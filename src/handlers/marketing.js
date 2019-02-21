const path = require('path');
const isScalar = require('is-scalar');
const fetch = require('node-fetch');
const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    EMAILOCTOPUS_API_KEY,
    EMAILOCTOPUS_LIST_ID,
    EMAILOCTOPUS_API_URL
} = process.env;

const required = ['email_address']

const email_regex = /(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])/i

const validateBody = body => {
    const keys = Object.keys(body);

    if (!keys.length) {
        throw new Error ('Missing Body data');
    }

    var present = required.filter(v => -1 !== keys.indexOf(v)).sort()
    if (present.toString() !== required.sort().toString()) {
        throw new Error ('missing required field');
    }

    keys.forEach(field => {
        // console.log(field, body[field]);
        if (!isScalar(body[field])) {
            throw new Error('Invalid body field. All fields must be scalar')
        }

        switch (field) {
            case 'email_address':
                if (!body[field].match(email_regex)) {
                    throw new Error('Invalid email address')
                }
                break;

            default:
                break;
        }
    })
}

const extractBody = event => {
    if (event.body) console.log('Event Body', event.body)
    if ( event.body && event.headers && (
        // not sure which is the problem, but either AWS or Postman is down casing the header... lame
        (event.headers[ 'Content-Type' ] && event.headers[ 'Content-Type' ] == 'application/json') ||
        (event.headers[ 'content-type' ] && event.headers[ 'content-type' ] == 'application/json')
    ) ) {
        const body = JSON.parse( event.body || '' )

        if (typeof body != 'object') {
            throw new Error ('request body must be a JSON object')
        }

        validateBody(body);

        console.log('parsed body', body)

        return body
    }

    throw new Error('Invalid request body.')
}

module.exports.email = async ( event, context ) => {
    // console.log('event', event);
    try {
        var body      = extractBody(event);
        var fields    = Object.keys(body);

    } catch (e) {
        console.log(e);
        return {
            statusCode: 400,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': true,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                code: 400,
                message: JSON.stringify(e.message)
            })
        }
    }

    try {
        var json = await fetch( EMAILOCTOPUS_API_URL + path.join('/', EMAILOCTOPUS_LIST_ID, 'contacts') , {
            method: 'post',
            body: JSON.stringify({
                api_key: EMAILOCTOPUS_API_KEY,
                email_address: body.email_address,
            }),
            headers: {
                'Content-Type': 'application/json'
            }
        })
	    .then( res => res.json() )

        return {
            statusCode: json.code || 500,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': true,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(json)
        }
    } catch (e) {
        console.log(e);
        return {
            statusCode: 500,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': true,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(e.message)
        }
    }
}
