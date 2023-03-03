import minify_html as _minify_html

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