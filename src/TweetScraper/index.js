const puppeteer = require('puppeteer-core');

const DEFAULT_OPTIONS = {
    launch: {
        // headless: false
    },
    pages: 99,
    timeline: 'light',
    replies: true,
    parents: true,
    quote: true,
    loadWait: 1000,
    pageWaitMode: 'networkidle0',
    pageTimeout: 3600,
    proxy: null,
    doScreenshot: false,
    screenshot: {
        type: "png",
    },
    ss_content_type: 'image/png',
    viewport: {
        width: 1280,
        height: 800
    },
    debug: false
}

const parseTweet = async (page, tweet, options) => {
    // console.log('tweet found');
    // console.log(tweet);

    const tweetData = await page.evaluate(element => {
        return {
            tweetId: element.getAttribute('data-tweet-id'),
            screenName: element.getAttribute('data-screen-name'),
            fullName: element.getAttribute('data-name'),
            permaLink: element.getAttribute('data-permalink-path'),
            userId: element.getAttribute('data-user-id'),
            retweetId: element.getAttribute('data-retweet-id'),
            retweeter: element.getAttribute('data-retweeter'),
            conversationId: element.getAttribute('data-conversation-id'),
            mentions: element.getAttribute('data-mentions') ? element.getAttribute('data-mentions').split(' ') : null,
            quality: element.getAttribute('data-conversation-section-quality'),
        };
    }, tweet);
    // consoleok.log(tweetData);

    const timestamp = await tweet.$eval('.tweet-timestamp span._timestamp',
        node => node.getAttribute('data-time'));
    const tweetText = await tweet.$eval('.tweet-text',
        node => node.innerText);
    const tweetHTML = await tweet.$eval('.tweet-text',
        node => node.innerHTML);
    const avatar = await tweet.$eval('.account-group img.avatar',
        node => node.getAttribute("src"));

    const images_t = await tweet.$$eval('.AdaptiveMedia-photoContainer',
        nodes => nodes.map(n => n.getAttribute('data-image-url')));
    const images_v = await tweet.$$eval('.AdaptiveMedia-photoContainer video',
        nodes => nodes.map(node => node.getAttribute('poster')));
    const images_b = await tweet.$$eval('.AdaptiveMedia-videoContainer .PlayableMedia-player',
        nodes => nodes.map(element => {
            let el = element.querySelector('.PlayableMedia-reactWrapper');
            if (el) el.setAttribute('style', 'display: none;');

            const sty = element.getAttribute('style');
            return sty.match(/:url\(\'(.+)\'\)/).pop();
        }));
    const images = images_t.concat(images_v, images_b);

    const retweet_num = await tweet.$eval('.stream-item-footer .ProfileTweet-action--retweet .ProfileTweet-actionCount',
        node => node.getAttribute('data-tweet-stat-count'));
    const favorite_num = await tweet.$eval('.stream-item-footer .ProfileTweet-action--favorite .ProfileTweet-actionCount',
        node => node.getAttribute('data-tweet-stat-count'));
    const reply_num = await tweet.$eval('.stream-item-footer .ProfileTweet-action--reply .ProfileTweet-actionCount',
        node => node.getAttribute('data-tweet-stat-count'));

    const quoteTweet = options.quote ? await tweet.$('.QuoteTweet .QuoteTweet-container .QuoteTweet-innerContainer')
        .then(async quote => {
            if (!quote) return null;
            if (options.debug) console.log('quote found');
            // console.log(quote);

            let quoteData = await page.evaluate(element => {
                return {
                    tweetId: element.getAttribute('data-item-id'),
                    screenName: element.getAttribute('data-screen-name'),
                    permaLink: element.getAttribute('href'),
                    userId: element.getAttribute('data-user-id'),
                    conversationId: element.getAttribute('data-conversation-id'),
                    quality: element.getAttribute('data-conversation-section-quality'),
                };
            }, quote);
            // console.log(quoteData);

            const fullName = await quote.$eval('.tweet-content .QuoteTweet-originalAuthor .QuoteTweet-fullname',
                node => node.innerText);
            const quoteText = await quote.$eval('.tweet-content .QuoteTweet-text',
                node => node.innerText);
            const quoteHTML = await quote.$eval('.tweet-content .QuoteTweet-text',
                node => node.innerHTML);

            let images = await quote.$$eval('.tweet-content .QuoteMedia-photoContainer',
                nodes => nodes.map(n => n.getAttribute('data-image-url')));
            const images_v = await quote.$$eval('.tweet-content .QuoteMedia-videoPreview img',
                nodes => nodes.map(n => n.getAttribute('src')));
            images = images.concat(images_v);

            const mentions = await quote.$$eval('.tweet-content .QuoteTweet-authorAndText .ReplyingToContextBelowAuthor .username > b',
                nodes => nodes.map( node => node.innerText ));

            quoteData = Object.assign({}, quoteData, {fullName, mentions, quoteText, quoteHTML, images});
            // console.log(quoteData);

            return quoteData;
        }).catch(err => {
            console.log('Quote Error or Not Found', err);
        }) : null;

    const screenshot = options.doScreenshot ? await tweet.screenshot(Object.assign({}, {
        // path: 'test.png',
        type: "png",
    }, options.screenshot || null)) : null;

    const out = Object.assign({}, tweetData, {avatar, timestamp, tweetText, tweetHTML, images, retweet_num,
            favorite_num, reply_num, quoteTweet, screenshot: screenshot ?
                'data:'+ (options.ss_content_type || 'image/png') + ';base64,' + screenshot.toString('base64') : null });
    // console.log(out);

    return out;
};

const loadStream = async (page, container, options) => {
    // console.log(container);

    await container.$('div.stream-container > div.stream > .stream-items li[data-element-context] > button')
        .then(async item => {
            if (item) {
                await item.click();
            }
        });

    let position = null;
    await container.$('div.stream-container > div.stream > div.stream-footer')
        .then(async footer => {

            const position_cb = async () => {
                return await container.$eval('div.stream-container',
                    node => node.getAttribute('data-min-position'))
                    .catch(err => {
                        console.log("Cannot Find Data Position Error: ", err);
                    });
            };

            let pages = (options.pages || 1) - 1;
            let position = await position_cb();
            let oldp = null;

            while (position != oldp && pages > 0) {
                oldp = position;
                await footer.hover()
                    .then(async () => {
                        // console.log('Seek.');
                        await page.waitFor(options.loadWait || 1000);
                    })
                    .catch(err => {
                        console.log("Page Seek Error: ", err);
                    });

                    position = await position_cb();
                    pages--;
            }
    });

    await container.$('div.stream-container > div.stream > .stream-items li[data-element-context] > button')
        .then(async item => {
            if (item) {
                await item.click();
            }
        });

    await container.$$('div.stream-container > div.stream > .stream-items li[data-expansion-url] > a')
        .then(async items => {
            for (var i in items) {
                await items[i].click().catch(err => {
                    console.log(`Click ${i} Failed.`, err);
                });
                await page.waitFor(50);
            }
        })
        .catch(err => {
            console.log('Hidden Reply Expansion Error.', err);
        });
}

const parseContainer = async (page, container, options) => {
    // console.log('container found');
    // console.log(container);

    let tweetData = await container.$('.permalink-tweet-container > div.tweet')
        .then(tweet => {
            return parseTweet(page, tweet, options);
        })
        .catch(err => {
            console.log("Tweet Parsing Failed.");
            throw err;
        });
    // console.log(tweetData);

    let parent_list = null,
        reply_list = null,
        parents = null,
        replies = null;

    if (options.timeline != false) {
        await container.$('.permalink-replies div#descendants').then(async replies => {
            await loadStream(page, replies, options);
        });

        parent_list = options.parents ? await container.$$eval('.permalink-in-reply-tos div#ancestors .stream-items li[data-item-id]',
            nodes => nodes.map(node => node.getAttribute('data-item-id'))) : null;

        reply_list = options.replies ? await container.$$eval('.permalink-replies div#descendants .stream-items li[data-item-id]',
            nodes => nodes.map(node => {
                if (node.getAttribute('data-retweet-id')) {
                    return node.getAttribute('data-retweet-id');
                }
                return node.getAttribute('data-item-id');
            })) : null;

        const map_tweets = async nodes => {
            for (var i in nodes) {
                nodes[i] = await parseTweet(page, nodes[i], options);
            }
            return nodes;
        };

        replies = options.timeline == 'full' && options.replies ?
            await container.$$('.permalink-replies div#descendants .stream-items li[data-item-id] .tweet')
                .then(map_tweets) : null;

        parents = options.timeline == 'full' && options.parents ?
            await container.$$('.permalink-in-reply-tos div#ancestors .stream-items li[data-item-id] .tweet')
                .then(map_tweets) : null;
    }

    tweetData = Object.assign({}, tweetData, {parent_list, reply_list});

    const screenshot = options.doScreenshot ? await container.screenshot(Object.assign({}, {
        type: "png",
    }, options.screenshot || null)) : null;

    return {tweetData, parents, replies, screenshot: screenshot ?
        'data:'+ (options.ss_content_type || 'image/png') + ';base64,' + screenshot.toString('base64') : null};
};

const parseTimeline = async (page, container, options) => {

    await loadStream(page, container, options);

    const map_tweets = async nodes => {
        for (var i in nodes) {
            nodes[i] = await parseTweet(page, nodes[i], options);
        }
        return nodes;
    };

    let tweet_list;

    if (options.timeline == 'full') {
        tweet_list = await container.$$('.stream-items li[data-item-id] .tweet')
            .then(map_tweets);

    } else if (options.timeline == 'light') {
        tweet_list = await container.$$eval('.stream-items li[data-item-id]',
            nodes => nodes.map(node => {
                if (node.getAttribute('data-retweet-id')) {
                    return node.getAttribute('data-retweet-id');
                }
                return node.getAttribute('data-item-id');
            }));
    }
    // console.log(tweet_list);

    return tweet_list;
};

class ScrapeTweet {

    constructor(props) {
        props.options = Object.assign({}, DEFAULT_OPTIONS, props.options || null);

        this.page = props.page || null;
        this.options = props.options || null;
    }

    async close() {
        if (this.browser) {
            return this.browser.close();
        }
    }

    async getBrowser() {
        if (!this.browser && this.page) {
            this.browser = this.page.browser();
        }

        if (!this.browser) {
            const opts = Object.assign({}, {
                args: ['--disable-gpu', '--no-sandbox', '--single-process',  '--disable-web-security',
                        '--disable-dev-profile', '--disable-dev-shm-usage', '--no-zygote'].concat(this.options.proxy ? [
                            `--proxy-server=${this.options.proxy.url}`
                        ] : []).concat(this.options.args || []),
                ignoreHTTPSErrors: true
            }, this.options ? (this.options.launch || null) : null);

            console.log(opts);

            this.browser = await puppeteer.launch(opts);
        }

        return this.browser;
    }

    async getPage() {
        if (!this.page) {
            const browser = await this.getBrowser();
            this.page = await browser.newPage();
            if (this.options.proxy) {
                this.page.authenticate({
                    username: this.options.proxy.username,
                    password: this.options.proxy.password
                })
            }
            this.page.setViewport(Object.assign({}, {
                width: 1280,
                height: 800
            }, this.options ? (this.options.viewport || null) : null));

            // twitter doesn't take kindly to linux
            let userAgent = await browser.userAgent();
            userAgent = userAgent.replace(/(Mozilla\/\d+\.\d+\s+)\([^\)]+\)/i, '$1(Windows NT 10.0; Win64; x64)');
            userAgent = userAgent.replace(/HeadlessChrome/, 'Chrome');

            this.page.setUserAgent(userAgent);
        }

        // this.page.on('response', response => {
        //     console.log('response', response.request().resourceType(), response.url());
        // })

        return this.page;
    }

    async getTweet(url) {
        const page = await this.getPage();
        // console.log(page);

        return await page.goto(url, {
            waitUntil: this.options.pageWaitMode || 'load',
            timeout: (this.options.pageTimeout || 3600) * 1000
        })
            .then(async response => {
                if (!response.ok()) {
                    throw "Bad Response";
                }

                const container = await this.page.$('div.permalink-container')
                    .then(container => {
                        return parseContainer(this.page, container, this.options);
                    })
                    .catch(err => {
                        console.log('Container Parse Failed.', err);
                    });

                return container;

            }).catch(err => {
                console.log(err);
            });
    }

    async getTimeline(url) {
        const page = await this.getPage();

        return await page.goto(url, {
            waitUntil: this.options.pageWaitMode || 'load'
        })
            .then(async response => {
                if (!response.ok()) {
                    throw "Bad Response";
                }

                const container = await this.page.$('div#timeline')
                    .then(container => {
                        return parseTimeline(this.page, container, this.options);
                    })
                    .catch(err => {
                        console.log('Timeline Parse Failed.', err);
                    });

                const screenshot = this.options.doScreenshot ? await container.screenshot(Object.assign({}, {
                    type: "png",
                }, this.options.screenshot || null)) : null;

                return {tweets: container, screenshot: screenshot ?
                    'data:'+ (options.ss_content_type || 'image/png') + ';base64,' + screenshot.toString('base64') : null};

            }).catch(err => {
                console.log(err);
            });
    }

    async getSearch(url) {
        const page = await this.getPage();

        return await page.goto(url, {
            waitUntil: this.options.pageWaitMode || 'load'
        })
            .then(async response => {
                if (!response.ok()) {
                    throw "Bad Response";
                }

                const container = await this.page.$('div#timeline')
                    .then(container => {
                        // console.log(container);
                        return parseTimeline(this.page, container, this.options);
                    })
                    .catch(err => {
                        console.log('Timeline Parse Failed.', err);
                    });

                return container;

            }).catch(err => {
                console.log(err);
            });
    }
}

module.exports = ScrapeTweet;
