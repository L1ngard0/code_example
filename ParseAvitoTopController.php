<?php

namespace console\controllers;


use backend\models\AvitoParseOnCategories;
use backend\models\ClientPosition;
use backend\models\DynamicsAvito;
use backend\models\Product;
use backend\models\UserSettings;
use yii;
use yii\console\Controller;
use keltstr\simplehtmldom\SimpleHTMLDom as SHD;
use console\models\Proxy;
use DateTime;

class ParseAvitoTopController extends Controller {
    public $str_proxy;
    public $cur_proxy_id = -1;
    public $max_proxy_id;
    public $proxy;
    public $use_proxy = true;
    public $stop = false;
    public $log_fileName = "";
    public $lock_file;
    public $use_lock_file = true;
    public $date_start;//будем использовать для различия запущеных функций в логе
    protected $sleep = false;
    public $dadata_keys = [
        [//support@alisa.market
            "app_key" => "670121df3cf1a3842538655e53bd2026e",
            "secret_key" => "9d23e6bb23d581579e807411fc3e7a4f9",
        ],
        [
            "app_key" => "bc0bf8c1d46b7384d1ea375a3d9a0c4d9f16c",
            "secret_key" => "312abf32689c373b4a75838e9c8d3e3db5",
        ],
    ];

    function actionIndex($url = "https://www.avito.ru/kirovskaya_oblast_kirov/avtomobili"){
        $this->log_fileName = Yii::getAlias("@root_folder")."/logs/cron/ParseAvitoOnCategories_top.txt";

        if ($this->use_lock_file) {
            @mkdir(Yii::getAlias("@root_folder") . "/logs/parse");
            $this->lock_file = fopen(Yii::getAlias("@root_folder") . "/logs/parse/avito__" . crc32($url) . "_top", 'w+');
            if (@flock($this->lock_file, LOCK_EX | LOCK_NB)) {//необходимо что бы уменьшить нагрузку на сервак. Парсинг одной и той же страницы не будет выполнятся одновременно более чем 1 раз
                fwrite($this->lock_file, date("Y-m-d H:i:s"));
            } else {
                $this->log_to_file("Париснг этой страницыы уже выполняется\n");
                return;
            }
        }

        $this->date_start = microtime(true);
        $this->get_next_proxy();
        $this->log_to_file("start\n\n\n");
        $pattern_AdId = "/_\\d+$/";
        /**/
        do {
            $html = $this->get_parse_object($url);
            if ($html != false) {
                if (count($html->find('div[data-marker="item"]')) == 0) {
                    echo "Не могу парсить такие ссылки, укажите страницу авито с пагинацией\n";
                    return;
                }
                $products = $html->find('div[data-marker="item"]');
                echo "Нашли ".count($products)." товаров на странице\n";
                $pos = 0;
                foreach ($products as $item) {
                    $pos++;
                    if($pos>10){
                        break;
                    }
                    try {
                        $product = [];

                        if (count($item->find('a[data-marker="item-link"]')) > 0) {//snippet-linkitem-description-title-link
                            $product["link"] = "https://www.avito.ru" . $item->find('a[data-marker="item-link"]', 0)->attr["href"];

                            if (stripos($product["link"], "?") !== false) {
                                $clear_link = stristr($product["link"], '?', true);
                            } else {
                                $clear_link = $product["link"];
                            }
                            echo "Ссылка на магазин ".$clear_link."\n";
                            $ourClient = UserSettings::findOne(["url_avito" => $clear_link]);
                            if(!empty($ourClient)){
                                echo "Наш клиент ".$ourClient->user_id."\n";
                                if (count($item->find("a")) > 0) {//snippet-linkitem-description-title-link
                                    $product["link"] = "https://www.avito.ru" . $item->find("a", 0)->attr["href"];

                                    if (stripos($product["link"], "?") !== false) {
                                        $clear_link_product = stristr($product["link"], '?', true);
                                    } else {
                                        $clear_link_product = $product["link"];
                                    }
                                    preg_match($pattern_AdId, $clear_link_product, $match);
                                    $product["AdId"] = (int)substr($match[0], 1);

                                }
                                $ourProduct = Product::findOne(["user_id" => $ourClient->user_id, "AdId" => $product["AdId"]]);
                                if(!empty($ourProduct)){
                                    echo "Наш товар ".$ourProduct->AdId."\n";
                                    $top = new ClientPosition();
                                    $top->AdId = $ourProduct->AdId;
                                    $top->user_id = $ourClient->user_id;
                                    $top->position = $pos;
                                    $top->date_parse = date("Y-m-d H:m:s");
                                    $top->query = $url;
                                    $top->save(false);
                                }
                            }

                        }

                    } catch (\Exception $e){
                        $this->log_to_file($e->getMessage());
                    }
                }
                $this->stop = true;
            } else {
                $this->log_to_file("Парсинг не удался, закончились прокси");
                if ($html === false) {
                    $this->log_to_file("Страница не найдена");
                    $this->stop = true;
                }
            }
        } while (!$this->stop);
        $this->log_to_file("Все спарсили".$this->stop);
        $this->log_to_file("finish\n\n\n");
        if ($this->use_lock_file) fclose($this->lock_file);
    }

    public function get_next_proxy() {
        $this->cur_proxy_id++;
        $field = "weight";
        $select = new \yii\db\Expression("`id`, `ip`, `$field`");
        $order_by = new \yii\db\Expression("`$field` DESC,`ozon_weight` DESC, `youla_weight`, `aliexpress_weight` DESC");

        $proxy_list = Proxy::find()->select($select)->where(['>', $field, -5])->orderBy($order_by)->limit(10)->all();

        $this->max_proxy_id = Proxy::find()->where(['>', $field, -5])->count();
        if ((count($proxy_list) == 0) or ($this->cur_proxy_id >= $this->max_proxy_id)) {
            $this->log_to_file("Закончились прокси, запросим новые");
            ProxyController::actionIndex();

            $this->cur_proxy_id = 0;

            $proxy_list = Proxy::find()->select($select)->where(['>', $field, -5])->orderBy($order_by)->limit(10)->all();

            $this->max_proxy_id = Proxy::find()->where(['>', $field, -5])->count();
            if ($this->max_proxy_id > -1) {
                if ($this->max_proxy_id > 9) {
                    $this->str_proxy = $proxy_list[rand(0,9)];
                } else {
                    $this->str_proxy = $proxy_list[rand(0, $this->max_proxy_id)];
                }
            } else {
                $this->log_to_file("Теперь точно все прокси кончились");
                $this->stop = true;
            }
        } else {
            $this->str_proxy = $proxy_list[rand(0, count($proxy_list) - 1)];
        }
        //        echo "\n" . date("Y-m-d-H-i-s") . " Переключаем прокси на " . $this->str_proxy->ip . "[" . $this->str_proxy->weight . "] \n";
    }

    public function get_parse_object($url, $maximum_page_length=200000){
        $time_limit = 40;

        if ($this->sleep) sleep(rand(1, 3));
        while($this->cur_proxy_id < $this->max_proxy_id) {
            //            if ($this->sleep) sleep(1);
            echo "\n";
            $this->log_to_file("Запрашиваем страницу: $url ");
            if ($this->use_proxy) echo date("Y-m-d H:i:s") . " Используем прокси " . $this->str_proxy->ip . "[" . $this->str_proxy->weight . "] \n";
            if ($ch = curl_init()) {
                $headers = [
                    'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
                    'accept-language: ru,en;q=0.9',
                    'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                    ':authority: www.avito.ru'
                ];
                $cookie = Yii::getAlias("@root_folder")."/logs/cookies/parse_aliexpress.txt";
                curl_setopt($ch,CURLOPT_RETURNTRANSFER, true);
                curl_setopt($ch,CURLOPT_FOLLOWLOCATION, true);
                curl_setopt($ch,CURLOPT_URL,$url);
                curl_setopt($ch,CURLOPT_HTTPHEADER,$headers);
                curl_setopt($ch,CURLOPT_SSL_VERIFYPEER, false);
                curl_setopt($ch,CURLOPT_SSL_VERIFYHOST, false);
                curl_setopt($ch,CURLOPT_COOKIEFILE, $cookie);
                curl_setopt($ch,CURLOPT_COOKIEJAR, $cookie);
                curl_setopt($ch, CURLOPT_TIMEOUT, $time_limit);
                curl_setopt($ch, CURLOPT_MAXREDIRS, 5);
                curl_setopt($ch, CURLOPT_REFERER, "www.avito.ru");
                if ($this->use_proxy) {
                    curl_setopt($ch, CURLOPT_PROXY, $this->str_proxy->ip); //"79.136.243.142:3128");
                    curl_setopt($ch, CURLOPT_PROXYTYPE, CURLPROXY_HTTP); //CURLPROXY_SOCKS5);
                }

                $html = curl_exec($ch);
                $errmsg  = curl_error($ch);
                curl_close($ch);
                $this->log_to_file("Длинна полученной страницы " . strlen($html));
                if ($html == "" and $errmsg == "") return false;
                if ($errmsg == "The requested URL returned error: 404 Not found") {
                    $this->log_to_file($errmsg);

                    return false;
                } elseif (strlen($html) < $maximum_page_length) {
                    if ($this->sleep) sleep(rand(1, 3));
                    if ($this->use_proxy) {
                        Proxy::updateAll(["weight" => $this->str_proxy["weight"] - 1], ["id" => $this->str_proxy["id"]]);
                        //                    echo date("Y-m-d H:i:s") . " Banned proxy: ".$this->str_proxy->id . " \n";

                        // этот прокси забанили - переключимся на новый и продолжим
                        $this->get_next_proxy();
                        if ($this->cur_proxy_id >= $this->max_proxy_id) {
                            $this->cur_proxy_id = 0;
                            echo "\n" . date("Y-m-d H:i:s") . " Переключаем прокси на $this->cur_proxy_id - сначала \n";
                        }
                    } else {
                        $this->log_to_file("Включили прокси");
                        $this->use_proxy = true;
                    }
                    continue;
                } else {
                    if ($this->use_proxy and $this->str_proxy["weight"] < 10) {
                        $this->str_proxy["weight"] += 1;
                        $this->str_proxy->save();
                    }
                }

                $html = SHD::str_get_html($html);


                return $html;

            } else {
                $this->log_to_file("curl no init");


                return false;
            }
        }

        // проксей больше не осталось - полный бан
        $this->log_to_file("Закончились прокси");
        $this->stop = true;


        return false;
    }

    public function log_to_file($str, $date=1){
        echo (($date)?date("Y-m-d H:i:s") . " ":"") . "$str \n";
        file_put_contents($this->log_fileName, (($date) ? date("Y-m-d H:i:s") : "") . "(" . $this->date_start . ") $str \n", FILE_APPEND | LOCK_EX);
    }
}