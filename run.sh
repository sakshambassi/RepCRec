if [ $# -eq 0 ]
  then
    python RepCRec/main.py --run-all
else
    python RepCRec/main.py --input $1    
fi