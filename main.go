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

	"github.com/gorilla/mux"
)

type Buyer struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type Products struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price string `json:"price"`
}

type Transactions struct {
	ID          string `json:"id"`
	Buyeid      string `json:"buyeid"`
	Ip          string `json:"ip"`
	Device      string `json:"device"`
	Productsids string `json:"productsids"`
}

type Stringer interface {
	String() string
}

func getBuyers() []Buyer {
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
	return fmt.Sprintln("{", " id:", "\"", ti.ID, "\"", ", name:", "\"", ti.Name, "\"", ", age:", ti.Age, "}")
}
func (ti Products) String() string {
	return fmt.Sprintln("{", " id:", "\"", ti.ID, "\"", ", name:", "\"", ti.Name, "\"", ", price:", ti.Price, "}")
}
func (ti Transactions) String() string {
	return fmt.Sprintln("{", " id:", "\"", ti.ID, "\"", ", buyeid: {id:", "\"", ti.Buyeid, "\"}", ", ip:", "\"", ti.Ip, "\"", ", device:", "\"", ti.Device, "\"", ", productsids:[", ti.Productsids, "]}")
}

func PostCargarEndPoint(w http.ResponseWriter, r *http.Request) {
	/////////////////////////////////////////////////////////////////////
	////buyers
	var c string
	buyers := getBuyers()

	for _, te := range buyers {

		ti := Buyer{
			ID:   te.ID,
			Name: te.Name,
			Age:  te.Age,
		}

		c = strings.Join([]string{ti.String(), c}, ",")

	}

	c = strings.ReplaceAll(c, " ", "")

	c = strings.TrimRight(c, ",")

	// fmt.Println(c)
	c = "mutation MyMutation { addBuyers(input: [ " + c + "]) {buyers{id name age}}}"

	jsonData := map[string]string{"query": c}

	// fmt.Println(jsonData)

	jsonValue, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValue)
	request, err := http.NewRequest("POST", "https://proud-sound.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValue))
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

	fmt.Println(data)
	fmt.Println(string(data))

	//////////////////////////////////////////////////////////////////////
	////productos
	var p string
	products := getProducts()

	for _, row := range products {
		tip := Products{
			ID:    row[0],
			Name:  row[1],
			Price: row[2],
		}
		p = strings.Join([]string{tip.String(), p}, ",")
	}

	p = strings.ReplaceAll(p, " ", "")

	p = strings.TrimRight(p, ",")

	// 	fmt.Println(p)
	p = "mutation MyMutation { addProducts(input: [ " + p + "]) {products{id name price}}}"

	jsonDatap := map[string]string{"query": p}

	// 	fmt.Println(jsonDatap)

	jsonValuep, err := json.Marshal(jsonDatap)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// 	// fmt.Println(jsonValuep)
	requestp, err := http.NewRequest("POST", "https://proud-sound.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValuep))
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
	fmt.Println(datap)
	fmt.Println(string(datap))

	/////////////////////////////////////////////////////
	//Transacciones

	var t, st, cad, idt, idbt, ipt, dt, idpt, st2, cad2 string
	transacc := getTransactions()
	var pos, pos2, kk, j, jjj, jj int

	// fmt.Println("\nTODOS LOS REGISTROS", transacc)
	for _, row := range transacc {
		for i := 1; i <= 4; i++ {
			// fmt.Println("\n REGISTROS \n=>", i, row[i])
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
								cad2 = "{id:\"" + cad2 + "\"}"
								// fmt.Println("\n SUB-PARTE", cad2)
								idpt = strings.Join([]string{idpt, cad2}, ",")
								// fmt.Println("\n SUB-SUB-PARTE", idpt)
								if jj == 1 {
									idpt = strings.TrimLeft(idpt, ",")
								}
							}
						}
						idpt = strings.TrimRight(idpt, ",")

					}
				}
				jjj++
			}
			tit := Transactions{
				ID:          idt,
				Buyeid:      idbt,
				Ip:          ipt,
				Device:      dt,
				Productsids: idpt,
			}
			t = strings.Join([]string{tit.String(), t}, ",")
			// t = strings.ReplaceAll(t, " ", "")
		}
	}

	t = strings.ReplaceAll(t, " ", "")

	t = strings.TrimRight(t, ",")

	fmt.Println(t)

	t = "mutation MyMutation { addTransactions(input: [ " + t + "]) {transactions{id buyeid{id} device ip productsids{id}}}}"

	jsonDatat := map[string]string{"query": t}

	fmt.Println(jsonDatat)

	jsonValuet, err := json.Marshal(jsonDatat)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValuep)
	requestt, err := http.NewRequest("POST", "https://proud-sound.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValuet))
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
	fmt.Println(datat)
	fmt.Println(string(datat))
}

func GetListarEndPoint(w http.ResponseWriter, r *http.Request) {
	c := "query MyQuery { queryBuyers {id name age}}"

	jsonData := map[string]string{"query": c}

	// fmt.Println(jsonData)

	jsonValue, err := json.Marshal(jsonData)
	if err != nil {
		fmt.Println("hay un error en el json")
		panic(err)
	}
	// fmt.Println(jsonValue)
	request, err := http.NewRequest("POST", "https://proud-sound.us-east-1.aws.cloud.dgraph.io/graphql", bytes.NewBuffer(jsonValue))
	if err != nil {
		fmt.Println("error al adicionar en el post")
		panic(err)
	}

	request.Header.Add("content-type", "application/json")
	request.Header.Add("Access-Control-Allow-Origin", "*")
	request.Header.Add("Access-Control-Allow-Methods", "DELETE, POST, GET, OPTIONS")
	request.Header.Add("Access-Control-Allow-Headers", "Content-Type")
	request.Header.Add("cache-control", "no-cache")
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

	// fmt.Println(data)
	fmt.Println(string(data))
	w.Write(data)
}

func PostListarUnoEndPoint(w http.ResponseWriter, r *http.Request) {
	// params := mux.Vars(r)

	// for _, item := range buyer {
	// 	if item.ID == params["id"] {
	// 		json.NewEncoder(w).Encode(item)
	// 		return
	// 	}
	// }
	// json.NewEncoder(w).Encode(&Buyers{})
}

func main() {

	r := mux.NewRouter()
	r.HandleFunc("/cargar", PostCargarEndPoint).Methods("POST")
	r.HandleFunc("/listar", GetListarEndPoint).Methods("GET", "OPTIONS")
	r.HandleFunc("/comprador/{id}", PostListarUnoEndPoint).Methods("GET")
	log.Fatal(http.ListenAndServe(":9000", r))

}
