package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

type Buyer struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Datec string `json:"datec "`
}

type Products struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price string `json:"price"`
	Datec string `json:"datec "`
}

type Transactions struct {
	ID          string `json:"id"`
	Buyeid      Buyer  `json:"buyeid"`
	Ip          string `json:"ip"`
	Device      string `json:"device"`
	Productsids string `json:"productsids"`
	Datec       string `json:"datec "`
}

type Stringer interface {
	String() string
}

func getBuyers() []Buyer {
	// EXTRAE LOS DATOS DEL ARCHIVO COMPRADOR
	buyers := make([]Buyer, 3)
	raw, err := ioutil.ReadFile("filebuyers.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	json.Unmarshal(raw, &buyers)
	return buyers
}

func getProducts() [][]string {
	// EXTRAE LOS DATOS DEL ARCHIVO PRODUCTOS
	fileName := "fileprods.cvs"
	fs1, _ := os.Open(fileName)

	r1 := csv.NewReader(fs1)
	r1.Comma = '\''
	content, err := r1.ReadAll()

	if err != nil {
		log.Fatalf("can not readall, err is %+v", err)
	}
	return content
}

func getTransactions() [][]string {
	// EXTRAE LOS DATOS DEL ARCHIVO DE TRANSACCIONES

	fileName := "filetrans.cvs"
	fs1, _ := os.Open(fileName)

	r1 := csv.NewReader(fs1)
	r1.Comma = '#'
	content, err := r1.ReadAll()

	if err != nil {
		log.Fatalf("can not readall, err is %+v", err)
	}

	return content
}

func (ti Buyer) String() string {
	// CONVIERTE EN UN STRING AL TYPE COMPRADOR
	fecha := time.Now().Format("2006-01-02T15:04:05")

	return fmt.Sprintln("{", " id:", "\"", ti.ID, "\"", ", name:", "\"", ti.Name, "\"", ", age:", ti.Age, ", datec:", "\"", fecha, "\"", "}")
}
func (ti Products) String() string {
	// CONVIERTE EN UN STRING AL TYPE PRODUCTO
	fecha := time.Now().Format("2006-01-02T15:04:05")
	aux := fmt.Sprintln("{", " id:", "\"", ti.ID, "\"", ", name:", "\"", ti.Name, "\"", ", price:", "\"", ti.Price, "\"", ", datec:", "\"", fecha, "\"", "}")

	return aux
}
func (ti Transactions) String() []string {
	// CONVIERTE EN UN STRING  AL TYPE TRANSACCION
	fecha := time.Now().Format("2006-01-02T15:04:05")
	ta := make([]string, 1)
	// fmt.Sprintln("{", " id:", "\"", ti.ID, "\"", ", buyeid:", ti.Buyeid, ", ip:", "\"", ti.Ip, "\"", ", device:", "\"", ti.Device, "\"", ", productsids:[", ti.Productsids, "], datec:", "\"", fecha, "\"", "}")

	a := fmt.Sprintln("{", "id:", "\"", ti.ID, "\"")
	b := strings.ReplaceAll(fmt.Sprintln("buyeid:", ti.Buyeid), " ", "")
	c := fmt.Sprintln("ip:", "\"", ti.Ip, "\"")
	d := fmt.Sprintln("device:", "\"", ti.Device, "\"")
	e := fmt.Sprintln("productsids:[", ti.Productsids, "]")
	f := fmt.Sprintln("datec:", "\"", fecha, "\"", "}")
	ta = append(ta, a, b, c, d, e, f)
	return ta
}

func GetCargarCompradorEndPoint(w http.ResponseWriter, r *http.Request) {
	/////////////////////////////////////////////////////////////////////
	////CARGA SOLO COMPRADORES
	var c string
	buyers := getBuyers()

	for _, te := range buyers {

		ti := Buyer{
			ID:    te.ID,
			Name:  te.Name,
			Age:   te.Age,
			Datec: te.Datec,
			// Transaction: "null",
		}

		c = strings.Join([]string{ti.String(), c}, ",")

	}

	c = strings.ReplaceAll(c, " ", "")

	c = strings.TrimRight(c, ",")

	c = "mutation MyMutation { addBuyers(input: [ " + c + "]) { numUids }}"

	fmt.Println(c)

	jsonData := map[string]string{"query": c}

	fmt.Println(jsonData)

	jsonValue, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValue)
	request, err := http.NewRequest("POST", "https://morning-paper.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValue))
	if err != nil {
		fmt.Println("error al adicionar en el post")
		panic(err)
	}
	// fmt.Println(request)
	request.Header.Add("content-type", "application/json")
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println("error al adicionar la query")
		panic(err)
	}
	// fmt.Println(response)
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("error en el body")
		panic(err)
	}

	// fmt.Println(data)
	// fmt.Println(string(data))
	w.Write(data)
}

func GetCargarProductoEndPoint(w http.ResponseWriter, r *http.Request) {
	//////////////////////////////////////////////////////////////////////
	////CARGA SOLO PRODUCTOS
	var p string
	products := getProducts()

	for _, row := range products {
		tip := Products{
			ID:    row[0],
			Name:  row[1],
			Price: row[2],
			Datec: "",
		}
		p = strings.Join([]string{tip.String(), p}, ",")
	}

	p = strings.ReplaceAll(p, " ", "")

	p = strings.TrimRight(p, ",")

	// 	fmt.Println(p)
	p = "mutation MyMutation { addProducts(input: [ " + p + "]) {numUids}}"

	jsonDatap := map[string]string{"query": p}

	// 	fmt.Println(jsonDatap)

	jsonValuep, err := json.Marshal(jsonDatap)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// 	// fmt.Println(jsonValuep)
	requestp, err := http.NewRequest("POST", "https://morning-paper.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValuep))
	if err != nil {
		fmt.Println("error al adicionar en el post")
		panic(err)
	}
	// 	// fmt.Println(requestp)
	requestp.Header.Add("content-type", "application/json")
	clientp := &http.Client{}
	responsep, err := clientp.Do(requestp)
	if err != nil {
		fmt.Println("error al adicionar la query")
		panic(err)
	}
	// 	// fmt.Println(responsep)
	defer responsep.Body.Close()

	datap, err := ioutil.ReadAll(responsep.Body)
	if err != nil {
		fmt.Println("error en el body")
		panic(err)
	}
	// fmt.Println(datap)
	// fmt.Println(string(datap))
	w.Write(datap)
}
func GetCargarTransactionsEndPoint(w http.ResponseWriter, r *http.Request) {
	/////////////////////////////////////////////////////
	//CARGA TRASACCIONES-COMPRADORES-PRODUCTOS
	var item Buyer
	var t, st, cad, idt, idbt, ipt, dt, idpt, st2, cad2 string
	transacc := getTransactions()

	var pos, pos2, kk, j, jjj, jj int
	buyers := getBuyers()
	products := getProducts()
	ta := make([]string, 1)
	// fmt.Println("\nTODOS LOS REGISTROS", transacc)
	for _, row := range transacc {
		// fmt.Println(row)
		x := 26
		for i := 1; i <= x; i++ {
			fmt.Println("\n REGISTROS \n=>", i, row[i])
			// fmt.Println("i=>", i) ////////////////7
			st = row[i]
			jjj = 1
			// k = strings.Count(st, " ")
			// fmt.Println("k=>", k)
			for j = 1; j <= 5; j++ {
				pos = strings.Index(st, " ")
				// fmt.Println("POSSSSS=>", pos)
				if pos > -1 {
					// fmt.Println("j=>", j) ////////////////
					cad = st[:pos]
					// fmt.Println("\n PARTE=>", j, cad)
					st = st[pos+1:]
					if jjj == 1 {
						idt = cad
					} else if jjj == 2 {
						idbt = cad
					} else if jjj == 3 {
						ipt = cad
					} else if jjj == 4 {
						dt = cad
					} else if jjj == 5 {
						// fmt.Println("\n VALOR DE CAD:", cad)
						cad2 = ""
						jjj = 1
						st2 = cad
						idpt = ""
						st2 = strings.ReplaceAll(st2, "(", "")
						st2 = strings.ReplaceAll(st2, ")", ",")
						kk = strings.Count(st2, ",")
						// fmt.Println("j=>", kk)
						// fmt.Println("\nKK", kk)
						for jj = 1; jj <= kk; jj++ {
							// fmt.Println("\njj", jj)
							pos2 = strings.Index(st2, ",")
							if pos2 > -1 {
								cad2 = st2[:pos2]
								st2 = st2[pos2+1:]
								// pos2 = strings.Index(st2, ",")
								///////////////////////////////////////////
								for _, row := range products {
									if row[0] == cad2 {
										fecha := time.Now().Format("2006-01-02T15:04:05")
										cad2 = "{" + fmt.Sprintln("id:", "\"", row[0], "\"") + fmt.Sprintln(", name:", "\"", strings.ReplaceAll(row[1], " ", "*"), "\"") + fmt.Sprintln(", price:", "\"", row[2], "\"") + fmt.Sprintln(", datec:", "\"", fecha, "\"") + "}"
										// fmt.Println("\n SUB-PARTE", cad2)
										idpt = strings.Join([]string{idpt, cad2}, ",")
										// fmt.Println("\n SUB-SUB-PARTE", idpt)
										if jj == 1 {
											idpt = strings.TrimLeft(idpt, ",")
										}

									}
								}

							}

						}
						idpt = strings.TrimRight(idpt, ",")
					}
				}
				jjj++
			}

			for _, item = range buyers {
				if item.ID == idbt {
					tit := Transactions{
						ID:          idt,
						Buyeid:      item,
						Ip:          ipt,
						Device:      dt,
						Productsids: idpt,
						Datec:       "",
					}
					ta = append(ta, strings.Join(tit.String(), ","))

					// return
				}
			}
			// tit := Transactions{
			// 	ID:          idt,
			// 	Buyeid:      item,
			// 	Ip:          ipt,
			// 	Device:      dt,
			// 	Productsids: idpt,
			// 	Datec:       "",
			// }

			// t = strings.Join(, t}, ",")

			// ta = append(ta, strings.Join(tit.String(), ","))
			// t = strings.ReplaceAll(t, " ", "")
			// fmt.Println(ta)
		}
		// fmt.Println(ta)
	}
	t = strings.Join(ta, "")
	t = strings.ReplaceAll(t, " ", "")
	t = strings.ReplaceAll(t, "*", " ")
	t = strings.TrimLeft(t, ",")

	// fmt.Println(t)

	t = "mutation MyMutation { addTransactions(input: [ " + t + "]) {numUids}}"
	// fmt.Println(t)

	jsonDatat := map[string]string{"query": t}

	fmt.Println(jsonDatat)

	jsonValuet, err := json.Marshal(jsonDatat)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValuep)
	requestt, err := http.NewRequest("POST", "https://morning-paper.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValuet))
	if err != nil {
		fmt.Println("error al adicionar en el post")
		panic(err)
	}
	// fmt.Println(requestp)
	requestt.Header.Add("content-type", "application/json")
	clientt := &http.Client{}
	responset, err := clientt.Do(requestt)
	if err != nil {
		fmt.Println("error al adicionar la query")
		panic(err)
	}
	// fmt.Println(responsep)
	defer responset.Body.Close()

	datat, err := ioutil.ReadAll(responset.Body)
	if err != nil {
		fmt.Println("error en el body")
		panic(err)
	}
	// fmt.Println(datat)
	// fmt.Println(string(datat))
	w.Write(datat)
}

func GetListarCompradorEndPoint(w http.ResponseWriter, r *http.Request) {
	//MUESTRA TODOS LOS COMPRADORES
	c := "query MyQuery { queryBuyers {id name age datec transactionc(filter: {}) {id ip device datec productsids {id name price datec}}}}"

	jsonData := map[string]string{"query": c}

	// fmt.Println(jsonData)

	jsonValue, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValue)
	request, err := http.NewRequest("POST", "https://morning-paper.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValue))
	if err != nil {
		fmt.Println("error al adicionar en el post")
		panic(err)
	}
	request.Header.Add("content-type", "application/json")
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println("error al adicionar la query")
		panic(err)
	}
	fmt.Println(request)
	fmt.Println(response)
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("error en el body")
		panic(err)
	}
	// r.Header.Set("Access-Control-Allow-Origin", "*")
	w.Write(data)
	// fmt.Println(data)
	//fmt.Println(string(data))

}
func GetListarProductosEndPoint(w http.ResponseWriter, r *http.Request) {
	//MUESTRA TODOS LOS PRODUCTOS
	c := "query MyQuery { queryProducts {id name price}}"

	jsonData := map[string]string{"query": c}

	// fmt.Println(jsonData)

	jsonValue, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValue)
	request, err := http.NewRequest("POST", "https://morning-paper.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValue))
	if err != nil {
		fmt.Println("error al adicionar en el post")
		panic(err)
	}
	request.Header.Add("content-type", "application/json")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println("error al adicionar la query")
		panic(err)
	}
	fmt.Println(request)
	fmt.Println(response)
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("error en el body")
		panic(err)
	}
	// r.Header.Set("Access-Control-Allow-Origin", "*")
	w.Write(data)
	// fmt.Println(data)
	//fmt.Println(string(data))

}
func GetListarTrasnsactionsEndPoint(w http.ResponseWriter, r *http.Request) {
	//MUESTRA TODOS LOS COMPRADORES CON TODOS SUS DATOS
	c := "query MyQuery { queryTransactions {id device ip datec productsids{id name price datec} buyeid{id name age datec}}}"

	jsonData := map[string]string{"query": c}

	// fmt.Println(jsonData)

	jsonValue, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValue)
	request, err := http.NewRequest("POST", "https://morning-paper.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValue))
	if err != nil {
		fmt.Println("error al adicionar en el post")
		panic(err)
	}
	request.Header.Add("content-type", "application/json")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println("error al adicionar la query")
		panic(err)
	}
	fmt.Println(request)
	fmt.Println(response)
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("error en el body")
		panic(err)
	}

	w.Write(data)

}

func GetListarUnoEndPoint(w http.ResponseWriter, r *http.Request) {
	//MUESTRA LOS DATOS DE UN COMPRADOR
	params := mux.Vars(r)

	c := "query MyQuery  { queryBuyers (filter: {has: id, id: {eq: " + "\"" + params["id"] + "\"" + "}}) {id name age datec }}"

	jsonData := map[string]string{"query": c}

	// fmt.Println(jsonData)

	jsonValue, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValue)
	request, err := http.NewRequest("POST", "https://morning-paper.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValue))
	if err != nil {
		fmt.Println("error al adicionar en el post")
		panic(err)
	}
	request.Header.Add("content-type", "application/json")
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println("error al adicionar la query")
		panic(err)
	}
	fmt.Println(request)
	fmt.Println(response)
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("error en el body")
		panic(err)
	}

	w.Write(data)

}

func enableCORS(router *mux.Router) {
	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost")
	}).Methods(http.MethodOptions)
	router.Use(middlewareCors)
}

func middlewareCors(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, req *http.Request) {
			// Just put some headers to allow CORS...
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
			// and call next handler!
			next.ServeHTTP(w, req)
		})
}

func main() {

	r := mux.NewRouter()
	enableCORS(r)

	r.HandleFunc("/cargarc", GetCargarCompradorEndPoint).Methods("GET")     // s
	r.HandleFunc("/cargarp", GetCargarProductoEndPoint).Methods("GET")      // s
	r.HandleFunc("/cargart", GetCargarTransactionsEndPoint).Methods("GET")  //s
	r.HandleFunc("/listarc", GetListarCompradorEndPoint).Methods("GET")     //s
	r.HandleFunc("/listarp", GetListarProductosEndPoint).Methods("GET")     //s
	r.HandleFunc("/listart", GetListarTrasnsactionsEndPoint).Methods("GET") //s
	r.HandleFunc("/buyer/{id}", GetListarUnoEndPoint).Methods("GET")        //s
	log.Fatal(http.ListenAndServe(":9000", r))

}
