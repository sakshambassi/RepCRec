if [ $# -eq 2 ]
  then
    python RepCRec/main.py --run-all --input $2
else
    python RepCRec/main.py --input $1
fi