from twisted.web.resource import Resource
from genshi.template import TemplateLoader
import os
import cStringIO, gzip

class TemplateResource(Resource):

    isLeaf = True

    def __init__(self, path = None):
        self.path = path

    loader = TemplateLoader( search_path=[ os.path.join(os.path.dirname(__file__), '../web_templates') ] , auto_reload=True )

    def render_GET(self, request):
        if self.path is not None:
            content = self._render_template( self.path.replace("docs/", "") + ".genshi" )
        else:
            content = self._render_template( request.path.replace("docs/", "").strip("/") + ".genshi" )
        
        content = content.replace("\t", "")
        encoding = request.getHeader("accept-encoding")

        if encoding and "gzip" in encoding:

            zbuf = cStringIO.StringIO()
            zfile = gzip.GzipFile(None, 'wb', 9, zbuf)
            if isinstance( content, unicode ):
                zfile.write( unicode(content).encode("utf-8") )
            elif isinstance( content, str ):
                zfile.write( unicode(content, 'utf-8' ).encode("utf-8") )
            else:
                zfile.write( unicode(content).encode("utf-8") )


            zfile.close()
            request.setHeader("Content-encoding","gzip")
            return zbuf.getvalue()
        else:
            return content
        
        
    def _render_template(self, template, data=None):
        if data is None:
            data = {}
        t = self.loader.load( template )
        return t.generate( data=data ).render('xhtml', doctype='xhtml')

