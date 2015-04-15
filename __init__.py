#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)

__license__ = 'GPL v3'
__copyright__ = '2015, Jin, Heonkyu <heonkyu.jin@gmail.com>'
__docformat__ = 'restructuredtext en'

import time
from urllib import quote
from Queue import Queue, Empty

from lxml.html import fromstring, tostring

from calibre import as_unicode
from calibre.ebooks.metadata import check_isbn
from calibre.ebooks.metadata.sources.base import Source
from calibre.utils.icu import lower
from calibre.utils.cleantext import clean_ascii_chars

class NaverBook(Source):
    name = 'NaverBook'
    description = _('Downloads metadata and covers from book.naver.com')
    author = 'Jin, Heonkyu <heonkyu.jin@gmail.com>'
    version = (0, 0, 2)
    minimum_calibre_version = (0, 8, 0)

    capabilities = frozenset(['identify', 'cover'])
    touched_fields = frozenset(['title', 'authors', 'identifier:naverbook',
        'identifier:isbn', 'rating', 'comments', 'publisher', 'pubdate',
        'tags', 'series', 'languages'])
    has_html_comments = True
    supports_gzip_transfer_encoding = True

    BASE_URL = 'http://book.naver.com'
    MAX_EDITIONS = 5

    def config_widget(self):
        '''
        Overriding the default configuration screen for our own custom configuration
        '''
        from calibre_plugins.naverbook.config import ConfigWidget
        return ConfigWidget(self)

    def get_book_url(self, identifiers):
        naverbook_id = identifiers.get('naverbook', None)
        if naverbook_id:
            return ('naverbook', naverbook_id,
                    '%s/bookdb/book_detail.nhn?bid=%s' % (NaverBook.BASE_URL, naverbook_id))

    def create_query(self, log, title=None, authors=None, identifiers={}):
        isbn = check_isbn(identifiers.get('isbn', None))
        q = ''
        url = ''
        if isbn is not None:
            q = '&isbn=' + isbn
            url = '/search/search.nhn?serviceSm=advbook.basic&ic=service.summary' + q
        elif title or authors:
            title_tokens = list(self.get_title_tokens(title,
                                strip_joiners=False, strip_subtitle=True))
            author_tokens = self.get_author_tokens(authors, only_first_author=True)

            tokens = [quote(t.encode('utf-8') if isinstance(t, unicode) else t) 
                for t in title_tokens]
            tokens += [quote(t.encode('utf-8') if isinstance(t, unicode) else t) 
                for t in author_tokens]
            q += '&query=' + '+'.join(tokens)
            url = '/search/search.nhn?sm=sta_hty.book' + q

        if not url:
            return None

        log.info('Search from %s' %(url))
        return NaverBook.BASE_URL + url

    def get_cached_cover_url(self, identifiers):
        url = None
        naverbook_id = identifiers.get('naverbook', None)
        if naverbook_id is None:
            isbn = identifiers.get('isbn', None)
            if isbn is not None:
                naverbook_id = self.cached_isbn_to_identifier(isbn)
        if naverbook_id is not None:
            url = self.cached_identifier_to_cover_url(naverbook_id)

        return url

    def identify(self, log, result_queue, abort, title=None, authors=None,
            identifiers={}, timeout=30):
        '''
        Note this method will retry without identifiers automatically if no
        match is found with identifiers.
        '''
        matches = []
        # Unlike the other metadata sources, if we have a goodreads id then we
        # do not need to fire a "search" at Goodreads.com. Instead we will be
        # able to go straight to the URL for that book.
        naverbook_id = identifiers.get('naverbook', None)
        isbn = check_isbn(identifiers.get('isbn', None))
        br = self.browser
        if naverbook_id:
            matches.append('%s/bookdb/book_detail.nhn?bid=%s' % (NaverBook.BASE_URL, naverbook_id))
        else:
            query = self.create_query(log, title=title, authors=authors,
                    identifiers=identifiers)
            if query is None:
                log.error('Insufficient metadata to construct query')
                return
            try:
                log.info('Querying: %s' % query)
                response = br.open_novisit(query, timeout=timeout)
            except Exception as e:
                err = 'Failed to make identify query: %r' % query
                log.exception(err)
                return as_unicode(e)

            # For ISBN based searches we have already done everything we need to
            # So anything from this point below is for title/author based searches.
            if not isbn:
                try:
                    raw = response.read().strip()
                    #open('E:\\t.html', 'wb').write(raw)
                    raw = raw.decode('utf-8', errors='replace')
                    if not raw:
                        log.error('Failed to get raw result for query: %r' % query)
                        return
                    root = fromstring(clean_ascii_chars(raw))
                except:
                    msg = 'Failed to parse goodreads page for query: %r' % query
                    log.exception(msg)
                    return msg
                # Now grab the first value from the search results, provided the
                # title and authors appear to be for the same book
                self._parse_search_results(log, title, authors, root, matches, timeout)

        if abort.is_set():
            return

        if not matches:
            if identifiers and title and authors:
                log.info('No matches found with identifiers, retrying using only'
                        ' title and authors')
                return self.identify(log, result_queue, abort, title=title,
                        authors=authors, timeout=timeout)
            log.error('No matches found with query: %r' % query)
            return

        from calibre_plugins.naverbook.worker import Worker
        workers = [Worker(url, result_queue, br, log, i, self) for i, url in
                enumerate(matches)]

        for w in workers:
            w.start()
            # Don't send all requests at the same time
            time.sleep(0.1)

        while not abort.is_set():
            a_worker_is_alive = False
            for w in workers:
                w.join(0.2)
                if abort.is_set():
                    break
                if w.is_alive():
                    a_worker_is_alive = True
            if not a_worker_is_alive:
                break

        return None

    def _parse_search_results(self, log, orig_title, orig_authors, root, matches, timeout):
        search_result = root.xpath('//ul[@id="searchBiblioList"]/li/dl')
        if not search_result:
            return
        log.info(search_result[0])
        title_tokens = list(self.get_title_tokens(orig_title))
        author_tokens = list(self.get_author_tokens(orig_authors))

        def ismatch(title, authors):
            authors = lower(' '.join(authors))
            title = lower(title)
            match = not title_tokens
            for t in title_tokens:
                if lower(t) in title:
                    match = True
                    break
            amatch = not author_tokens
            for a in author_tokens:
                if lower(a) in authors:
                    amatch = True
                    break
            if not author_tokens: amatch = True
            return match and amatch

        matched_node = None
        for node in search_result:
            title = node.xpath('./dt/a')[0].text_content().strip()
            authors = map(lambda x: x.text_content(), node.xpath('./dd[@class="txt_block"]/a'))
            log.info('Iterating for %s (%s)' % (title, ','.join(authors)))
            if ismatch(title, authors):
                log.info('Matched')
                matched_node = node
                break

        if matched_node is None:
            log.error('Rejecting as not close enough match: %s %s' % (title, authors))
            return

#first_result_url_node = root.xpath('//table[@class="tableList"]/tr/td[1]/a[2]/@href')
        first_result_url_node = matched_node.xpath('./dt/a/@href')
        if first_result_url_node:
            import calibre_plugins.naverbook.config as cfg
            c = cfg.plugin_prefs[cfg.STORE_NAME]
            result_url = first_result_url_node[0]
            matches.append(result_url)

    def download_cover(self, log, result_queue, abort,
            title=None, authors=None, identifiers={}, timeout=30):
        cached_url = self.get_cached_cover_url(identifiers)
        if cached_url is None:
            log.info('No cached cover found, running identify')
            rq = Queue()
            self.identify(log, rq, abort, title=title, authors=authors,
                    identifiers=identifiers)
            if abort.is_set():
                return
            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break
            results.sort(key=self.identify_results_keygen(
                title=title, authors=authors, identifiers=identifiers))
            for mi in results:
                cached_url = self.get_cached_cover_url(mi.identifiers)
                if cached_url is not None:
                    break
        if cached_url is None:
            log.info('No cover found')
            return

        if abort.is_set():
            return
        br = self.browser
        log('Downloading cover from:', cached_url)
        try:
            cdata = br.open_novisit(cached_url, timeout=timeout).read()
            result_queue.put((self, cdata))
        except:
            log.exception('Failed to download cover from:', cached_url)


if __name__ == '__main__': # tests
    # To run these test use:
    # calibre-debug -e __init__.py
    from calibre.ebooks.metadata.sources.test import (test_identify_plugin,
            title_test, authors_test, series_test)

    test_identify_plugin(NaverBook.name,
        [
            (# A book with an ISBN
                {'identifiers':{'isbn': '9780385340588'},
                    'title':'61 Hours', 'authors':['Lee Child']},
                [title_test('61 Hours', exact=True),
                 authors_test(['Lee Child']),
                 series_test('Jack Reacher', 14.0)]
            ),

            (# A book throwing an index error
                {'title':'The Girl Hunters', 'authors':['Mickey Spillane']},
                [title_test('The Girl Hunters', exact=True),
                 authors_test(['Mickey Spillane']),
                 series_test('Mike Hammer', 7.0)]
            ),

            (# A book with no ISBN specified
                {'title':"Playing with Fire", 'authors':['Derek Landy']},
                [title_test("Playing with Fire", exact=True),
                 authors_test(['Derek Landy']),
                 series_test('Skulduggery Pleasant', 2.0)]
            ),

            (# A book with a Goodreads id
                {'identifiers':{'goodreads': '6977769'},
                    'title':'61 Hours', 'authors':['Lee Child']},
                [title_test('61 Hours', exact=True),
                 authors_test(['Lee Child']),
                 series_test('Jack Reacher', 14.0)]
            ),

        ])


