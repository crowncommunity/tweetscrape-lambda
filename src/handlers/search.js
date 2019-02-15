const AWS = require( 'aws-sdk' );

const ES = require( 'elasticsearch' );
const AWS_class = require( 'http-aws-es' );

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    INDEX_NAME,
    ELASTIC_HOST,
    ELASTIC_REGION
} = process.env;

const scroll_timeout = '3m';

const clientconfig = {
    hosts: ELASTIC_HOST,
	connectionClass: AWS_class,
	awsConfig: new AWS.Config( {
		region: ELASTIC_REGION,
		credentials: DEV_MODE ? new AWS.Credentials(
			AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY
		) : new AWS.EnvironmentCredentials( 'AWS' ),
	} )
}
const client = ES.Client( clientconfig )

const sTpl = (q, fields, boost = 1) => {
    fields = fields.map(i => {
        if (i.match(/^t\./)) {
            return i.replace(/^t\./, 'tweetData.');
        }
        if (i.match(/^q\./)) {
            return i.replace(/^q\./, 'tweetData.quoteTweet.');
        }
        return i;
    })
    if (Array.isArray(q)) {
        q = q.join(' ');
    }
    return {
        query_string: {
            fields: fields,
            query: q,
            boost: boost
        }
    }
}

const quoteQuote = str => {
    const r = m => {
        return "\\" + m;
    }
    const esc = str => {
        return str.replace(/[\(\)\[\]\{\}\!\&\|\:\\]/g, r)
    }
    str = str.replace(/^([^"]*")(?<=^[^"]*")(.*)(?="[^"]*$)("[^"]*$)|^([^"]*")(?<=^[^"]*")(.*)$|^(.*)(?="[^"]*$)("[^"]*$)/,
        (m, p1, p2, p3, p4, p5, p6, p7) => {
            if (p2) {
                return esc(p1) + esc(p2).replace(/(["])/g, "\\\"").replace(/\+\-/g, r) + esc(p3);
            }
            if (p4) {
                return esc(p4).replace(/(["])/g, "\\\"").replace(/\+\-/g, r) + esc(p5);
            }
            if (p6) {
                return esc(p6).replace(/(["])/g, "\\\"").replace(/\+\-/g, r) + esc(p7);
            }
            return esc(m).replace(/\+\-/g, r)
        })


    return str;
}

const betterTokenize = str => {
    const s = ' ';
    const q = '"';
    var c = '';
    var mode = s;
    var tokens = []
    str.trim().replace(/\s+/, ' ').split(/(?=[\s"])/i).forEach(i => {
        // if char is not space, append.
        // if char is quote, stay in quote mode until you see another quote

        if (i[0] == s && mode == s) {
            // console.log(c, '->', i);
            tokens.push(quoteQuote(c.trim()));
            c = i;
        } else if (i[0] == q && mode == s) {
            mode = q;
            c += i
        } else if (i[0] == q && mode == q) {
            mode = '';
            c += i
        } else if (i[0] == s && mode == '') {
            // console.log(c, '->', i);
            mode = s;
            tokens.push(quoteQuote(c.trim()));
            c = i
        } else {
            c += i;
        }

        // console.log("(" + i[0] + ")", "(" + mode + ")", i);
    });
    if (c) tokens.push(quoteQuote(c.trim()));

    return tokens;
}

module.exports.main = async (event, context) => {

    const fieldname = 'q';
    var queryString;
    var querySort;

    if (event.queryStringParameters) console.log('single parameters', event.queryStringParameters)
	if ( event.queryStringParameters ) {
        if (event.queryStringParameters[ fieldname ]) {
            queryString = event.queryStringParameters[ fieldname ];
            querySort = '_score,timestamp: desc'
        } else {
            queryString = '*'
            querySort = 'timestamp: desc'
        }
    }

    console.log('queryString', queryString);

    var rawq = betterTokenize(queryString || '');
    var must = rawq.filter(i => i.match(/^\+/))
        .map(i => i.replace(/^\+/, ''));
    var shld = rawq.map(i => i.replace(/^\+/, ''));

    console.log('tokenized', rawq);

    if (!must.length && !shld.length) {
        return { statusCode: 204 }
    }

    const parse2silos = tokens => {
        var tids = tokens.filter(i => i.match(/^"?\d+"?$/))
            .map(i => i.replace(/"/g, ''));
        var name = tokens.filter(i => i.match(/^@"?(\w+)"?$/))
            .map(i => i.replace(/^@"?(\w+)"?$/, (m, p1, o, s) => {
                return p1;
            }));
        var tags = tokens.filter(i => i.match(/^#"?(\w+)"?$/))
            .map(i => i.replace(/^#"?(\w+)"?$/, (m, p1, o, s) => {
                return p1;
            }));
        var mens = tokens.filter(i => i.match(/^(?:m(?:entions?)?|r(?:eplyto)?):"?@?(\w+)"?$/i))
            .map(i => i.replace(/^(?:m(?:entions?)?|r(?:eplyto)?):"?@?(\w+)"?$/i, (m, p1, o, s) => {
                return p1;
            }));
        var repl = tokens.filter(i => i.match(/^r(?:eplyto)?:"?@?(\d+)"?$/i))
            .map(i => i.replace(/^r(?:eplyto)?:"?@?(\d+)"?$/i, (m, p1, o, s) => {
                return p1;
            }));
        tokens = tokens.map(i => i.replace(/^(?:m(?:entions?)?|r(?:eplyto)?):/i, ''))
            .map(i =>  {
                if (!i.match(/"/)) {
                    return i.replace(/[\+\-\(\)\[\]\{\}\!\&\|\:\\]/g,  m => {
                        return "\\" + m;
                    });
                }
                return i;
            });

        return {
            tweetIds: tids,
            userNames: name,
            hashTags: tags,
            mentions: mens,
            inReplyTo: repl,
            normalized: tokens
        }
    }

    const silo2query = silo => {
        silo = parse2silos(silo);

        var q_userNames = silo.userNames && silo.userNames.length ? [
            sTpl(silo.userNames, ["t.screenName"], 2.5),
            sTpl(silo.userNames, ["q.screenName"], 1.75),
            sTpl(silo.userNames, ["t.mentions"], 1.5),
            sTpl(silo.userNames, ["q.mentions"], 1.25),
            sTpl(silo.userNames, ["t.fullName", "q.fullName"], 0.75),
            // sTpl(silo.userNames, ["t.fullName", "q.fullName", "t.tweetText"], 0.75),
            // sTpl(silo.userNames, ["q.tweetText"], 0.5),
        ] : [];

        var q_tweetIds = silo.tweetIds && silo.tweetIds.length ? [
            sTpl(silo.tweetIds, ["t.tweetId"], 2.5),
            sTpl(silo.tweetIds, ["q.tweetId", "t.tweetHTML"], 2.25),
            sTpl(silo.tweetIds, ["t.conversationId", "q.conversationId"], 1.5),
            sTpl(silo.tweetIds, ["q.tweetHTML"], 0.75),
        ] : [];

        // const qd_repl_h = i => {
        //     return '"' + (i.match(/^#/) ? i : ('#' + i)) + '"'
        // }
        var q_hashTags = silo.hashTags && silo.hashTags.length ? [
            sTpl(silo.hashTags, ["tags"], 3),
            sTpl(silo.hashTags, ["t.tweetText"], 2.5),
            sTpl(silo.hashTags, ["q.tweetText"], 2),
            // sTpl(silo.hashTags.map(i => qd_repl_h(i)), ["t.tweetText"], 2.5),
            // sTpl(silo.hashTags.map(i => qd_repl_h(i)), ["q.tweetText"], 2),
        ] : [];

        var q_mentions = silo.mentions && silo.mentions.length ? [
            sTpl(silo.mentions.concat(silo.userNames), ["t.mentions"], 3),
            sTpl(silo.mentions.concat(silo.userNames), ["q.mentions", "q.screenName"], 2.5),
        ] : [];

        var q_inReplyTo = silo.inReplyTo && silo.inReplyTo.length ? [
            sTpl(silo.inReplyTo, ["t.conversationId"], 3),
            // sTpl(silo.inReplyTo, ["t.reply_list", "t.parent_list"], 2.5), // not currently used
            sTpl(silo.inReplyTo, ["q.conversationId", "q.tweetId"], 2.5),
        ] : [];

        var q_normalized = silo.normalized && silo.normalized.length ? [
            sTpl(silo.normalized, ["t.screenName", "t.fullName"], 1.5),
            sTpl(silo.normalized, ["tags", "t.tweetText", "t.mentions"], 1.25),
            sTpl(silo.normalized, ["q.screenName", "q.fullName", "q.tweetText", "q.mentions"], 1),
            sTpl(silo.normalized, ["t.tweetId"], 0.75),
            sTpl(silo.normalized, ["t.conversationId", "q.conversationId"], 0.5),
        ] : [];

        return []
            .concat(q_tweetIds)
            .concat(q_userNames)
            .concat(q_hashTags)
            .concat(q_mentions)
            .concat(q_inReplyTo)
            .concat(q_normalized)
    }

    // console.log(silo2query(shld));

    var query = {
        query : {
            bool: {
                must: must.length ? silo2query(must) : null,
                should: shld.length ? silo2query(shld) : null
            }
        }
    }

    console.log('ES query', query)
    // console.log(JSON.stringify(query));

    var status
	var response = await client.search( {
        index: INDEX_NAME,
        body: query,
        scroll: scroll_timeout,
        size: 25,
        sort: querySort || null
    })
    .catch(err => {
        status = err.statusCode
        console.log(err)
    })

    if (response && response.hits)
        response.hits.scroll_id = response._scroll_id || null;

    // console.log(response);

    return {
        statusCode: (response && response.hits) ? 200 : (status || 500),
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
            'Content-Type': 'application/json'
        },
        body: response ? JSON.stringify(response.hits || {}) : null
    }
}

module.exports.scroll = async (event, context) => {
    const fieldname = 'scroll_id';
    var scroll_id

    if (event.queryStringParameters) console.log('single parameters', event.queryStringParameters)
    if ( event.queryStringParameters && event.queryStringParameters[ fieldname ]) {
        scroll_id = event.queryStringParameters[ fieldname ];
    }

    if (!scroll_id) {
        return { statusCode: 400 }
    }

    var status
    var response = await client.scroll( {
        scroll: scroll_timeout,
        scroll_id: scroll_id
    })
    .catch(err => {
        status = err.statusCode
        console.log(err)
    })

    if (response && response.hits)
        response.hits.scroll_id = response._scroll_id || null;

    // console.log(response);

    return {
        statusCode: (response && response.hits) ? 200 : (status || 500),
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
            'Content-Type': 'application/json'
        },
        body: response ? JSON.stringify(response.hits || {}) : null
    }

}

module.exports.stop = async (event, context) => {
    const fieldname = 'scroll_id';
    var scroll_id

    if (event.queryStringParameters) console.log('single parameters', event.queryStringParameters)
    if ( event.queryStringParameters && event.queryStringParameters[ fieldname ]) {
        scroll_id = event.queryStringParameters[ fieldname ];
    }

    if (!scroll_id) {
        return { statusCode: 400 }
    }

    var status
    var response = await client.clearScroll( {
        scrollId: [scroll_id]
    })
    .catch(err => {
        status = err.statusCode
        console.log(err)
    })

    return {
        statusCode: (response && response.succeeded) ? 204 : (status || 500),
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
            'Content-Type': 'application/json'
        },
        // body: JSON.stringify(response.hits || {})
    }

}
