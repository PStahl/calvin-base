apps=`cscontrol http://localhost:5002 applications list`
app_id=`echo $apps | perl -nle"print $& if m{(?<=u').*?(?=')}"`
cscontrol http://localhost:5002 applications delete $app_id
