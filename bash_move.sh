for f in *.json; do 
dst="datafiles/$f"
#echo $dst
mv "$f" "$dst"
done
