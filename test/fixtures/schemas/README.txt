# Avro Schemas README
# 
# Created: 3/10/2014
# Last modified: 3/31/2014
# Author: cmills

The schemas contained in this directory are intended for use in the first 
rollout of the HDFS event archive.  These schemas are used here for unit tests,
but will also be added to the schema repository in the same form as version 0 
of each event_type/topic specific schema.

The event types covered are: browse_click_tracking, gold, login_detail, 
message, newsfeed_clicks, and page_view.  These were chosen in part because of 
their varied structures, but also to include the events required to calculate
DAU (login and page_view).

Note that Kafka topic names are different than event types.  For each event type
we can have two topics -- one for the old, raw format messages and one for the 
new, serialized messages.  The old topic name is the same as the event type, but
the new topic name adds an "s_" prefix (e.g. -- the page_view event type is 
associated with both the "page_view" and "s_page_view" topics).

Generally speaking, the PHP code logs events to Bruce using the common function 
tag_log::log_message_pageview() (or the alias method tag_log::data()).  This 
method takes an array of values and sends them to a UNIX-domain datagram socket 
file (/var/run/bruce/bruce.socket) as a space-delimited sequence of text fields.
Examples of these messages as sent to Bruce are included in the 
fixtures/raw_samples directory.

Getting from a sequence of text fields to a typed, structured event object 
requires that we know type and meaning of each field.  The structure for each 
of these events is taken from usage in production PHP code from the "web" 
project and the related GreenPlum table where the events are currently stored.
The PHP code files and GP tables for each covered event type are detailed below.
This logic is here encapsulated in the R2D

Note that there are two additional schemas included: unreadable_event and 
envelope.  The envelope schema supports direct serialization of the sequence of 
text fields without any typing or field name assignment.  Envelope is useful as 
a default schema to be used when an event type-specific schema is unavailable.
The unreadable_event schema is a way for an event logger or processor to log 
bytes as received that were supposed to be interpretable as an event, but were
not.

browse_click_tracking
---------------------
FIELDS: <event_type>, <user_id>, <other_user_id>, <action>
PHP: /web/shared/class/tag/tracking.php; _log() method
GP: taganalysis.browse_click_tracking_log


gold
----
FIELDS: <tx_type>, <tx_amount>, <user_id>, <tx_id>, <new_balance>, <result>, 
        (<referrer>), <session_id>, (<processor_tx_id>), <bonus_amount>
PHP: /web/shared/class/tag/user/gold.php; tag_log::log_message()
GP: taganalysis.gold_log


login
-----
FIELDS: <user_id>, <status>, <ips>, <email>, <guid>, <login_type>, <session_id>,
        (<status_code>), (<country_code>), <browser_id>
PHP: /web/cool/res/savant/_modules/validate_captcha.php
PHP: /web/shared/class/tag/login.php; _post_failed_login(), _post_login_actions() 
GP: taganalysis.login_log
NOTES: The successful calls include a numeric status code and a country code 
that are not present if the login fails.


message
-------
FIELDS: <action>, <from_user_id>, <to_user_id>, <message_id>, <sent_timestamp>,
        <session_id>, <is_friend>
PHP: /web/shared/class/tag/api/tagged/im.php; markRead()
PHP: /web/shared/class/tag/message/folder.php; addMessage(), markAsViewed()
PHP: /web/shared/class/tag/security/msglimits.php; 
PHP: /web/shared/class/tag/user/im.php; _doPageviewLogging()
GP: taganalysis.message_log

newsfeed_clicks
---------------
FIELDS: <user_id>, <posting_user_id>, <click_timestamp>, <click_type>, 
        <click_source>
PHP: /web/shared/class/tag/api/tagged/newsfeed/app.php; click()
PHP: /web/shared/class/tag/api/tagged/newsfeed/user.php; hide(), click()
GP: taganalysis.newsfeed_clicks_log

page_view
---------
FIELDS: <request_uri>, <user_id>, <session_id>, <ips>, (<is_redirect>), <guid>,
        <domain>, <browser_id>
PHP: /web/shared/class/tag/log.php; tag_log::log_pageview_callback()
GP: taganalysis.page_view_log









