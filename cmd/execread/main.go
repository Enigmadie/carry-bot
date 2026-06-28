package main
import("context";"fmt";"github.com/nats-io/nats.go";"github.com/nats-io/nats.go/jetstream")
func main(){
 nc,_:=nats.Connect(nats.DefaultURL); defer nc.Close()
 js,_:=jetstream.New(nc)
 st,err:=js.Stream(context.Background(),"EXEC"); if err!=nil{fmt.Println("stream err",err);return}
 for seq:=uint64(1);seq<=5;seq++{
  m,err:=st.GetMsg(context.Background(),seq)
  if err!=nil{continue}
  fmt.Printf("seq=%d subj=%s data=%s\n",seq,m.Subject,string(m.Data))
 }
}
