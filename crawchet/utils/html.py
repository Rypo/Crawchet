import css_inline
import minify_html as _minify_html
import lxml.html

def html_minify(html_content:str, 
           do_not_minify_doctype=False,
           ensure_spec_compliant_unquoted_attribute_values=False,
           keep_closing_tags=True,
           keep_html_and_head_opening_tags=False,
           keep_spaces_between_attributes=False,
           keep_comments=False,
           minify_css=False,
           minify_js=False, 
           remove_bangs=True,
           remove_processing_instructions=False):
    ''' Wrapper for github.com/wilsonzlin/minify-html
    
    Args:
        html_content: str
            html text to minified
        do_not_minify_doctype: bool
            Do not minify DOCTYPEs. Minified DOCTYPEs may not be spec compliant.

        ensure_spec_compliant_unquoted_attribute_values: bool
            Ensure all unquoted attribute values in the output do not contain any characters prohibited by the WHATWG specification.

        keep_closing_tags: bool
            Do not omit closing tags when possible.

        keep_html_and_head_opening_tags: bool
            Do not omit <html> and <head> opening tags when they don’t have attributes.

        keep_spaces_between_attributes: bool
            Keep spaces between attributes when possible to conform to HTML standards.

        keep_comments: bool
            Keep all comments.

        minify_css: bool
            If enabled, CSS in <style> tags and style attributes are minified.

        minify_js: bool
            If enabled, JavaScript in <script> tags are minified using minify-js.
            Only <script> tags with a valid or no MIME type is considered to contain JavaScript, as per the specification.

        remove_bangs: bool
            Remove all bangs.

        remove_processing_instructions: bool
            Remove all processing_instructions.
    '''

    return _minify_html.minify(html_content,
                    do_not_minify_doctype=do_not_minify_doctype,
                    ensure_spec_compliant_unquoted_attribute_values=ensure_spec_compliant_unquoted_attribute_values,
                    keep_closing_tags=keep_closing_tags,
                    keep_html_and_head_opening_tags=keep_html_and_head_opening_tags,
                    keep_spaces_between_attributes=keep_spaces_between_attributes,
                    keep_comments=keep_comments,
                    minify_css=minify_css,
                    minify_js=minify_js, 
                    remove_bangs=remove_bangs,
                    remove_processing_instructions=remove_processing_instructions)


def try_make_absolutelinks(html_content, base_url, decode=False, errors='ignore'):
    if not isinstance(html_content, bytes):
        html_content = html_content.encode()

    if html_content == b'':
        return html_content

    try:
        html_content = lxml.html.make_links_absolute(html_content, base_url=base_url)#, handle_failures='ignore')
        #html_content = lxml.html.clean.autolink_html(html_content) # this breaks encoding for some reason
    except Exception as e:
        if errors == 'ignore':
            pass
        elif errors == 'print':
            print(e)
        elif errors == 'raise':
            raise e
        
    if decode:
        html_content = html_content.decode()#errors='ignore')

    return html_content

def try_inline_css(html_content, fetch_remote_css=False, remove_style_tags=True, errors='ignore'):
    ''' Note: Only UTF-8 for string representation. Other document encodings are not yet supported.'''
    try:
        # Only UTF-8 for string representation. Other document encodings are not yet supported.
        html_content = css_inline.inline(html_content, remove_style_tags=remove_style_tags, load_remote_stylesheets=fetch_remote_css) # base_url=url,
    except Exception as e:
        if fetch_remote_css:
            try: 
                html_content = css_inline.inline(html_content, remove_style_tags=remove_style_tags, load_remote_stylesheets=False)
            except Exception as e:
                if errors == 'ignore':
                    pass
                elif errors == 'print':
                    print(e)
                elif errors == 'raise':
                    raise e
                

    return html_content

def try_minify_html(html_content, errors='ignore', **kwargs):
    try:
        html_content = html_minify(html_content, **kwargs)
    except Exception as e:
        if errors == 'ignore':
            pass
        elif errors == 'print':
            print(e)
        elif errors == 'raise':
            raise e

    return html_content