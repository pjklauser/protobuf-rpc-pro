pre-requisite perl, node.js 
install hugo ( https://gohugo.io/ )
in the doc/theme folder - install hugo themes.
  https://github.com/digitalcraftsman/hugo-material-docs
	git clone https://github.com/digitalcraftsman/hugo-material-docs.git themes/hugo-material-docs
  adapt the doc/themes/config.toml to point the "theme" property to the theme name
  	
from within the doc folder

> hugo serve

if happy with resulting documentation

> hugo -d ../../docs

to deploy to the documentation github folder.