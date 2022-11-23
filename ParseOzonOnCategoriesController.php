<?php

namespace console\controllers;


use backend\models\DynamicsOzon;
use backend\models\OzonCompetitorProduct;
use backend\models\OzonParseOnCategories;
use backend\models\OzonProduct;
use PHPUnit\Exception;
use yii;
use yii\console\Controller;
use keltstr\simplehtmldom\SimpleHTMLDom as SHD;
use console\models\Proxy;
//use console\models\ProxyWithoutRussia as Proxy;//Если будут нужны российские прокси, то убери часть " as Proxy"

class ParseOzonOnCategoriesController extends Controller {
    public $str_proxy;
    public $cur_proxy_id = -1;
    public $max_proxy_id;
    public $proxy;
    public $stop = false;
    public $log_fileName = "";
    public $lock_file;
    public $use_lock_file = true;
    public $date_start;//будим использовать для различия запущеных функций в логе

    public $sleep_for_db_exception = 30;

    public $cookie = ["tmr_reqNum" => "1"];

    public $use_proxy = true;

    public function actionHelp() {
        echo "
    Функция php yii parse-ozon-on-categories...
    принимает следующие параметры в указанном порядке:
        
        * string \$url Полная ссылка на запрос пример: \"https://www.ozon.ru/catalog/0/search.aspx?search=%D1%88%D0%B0%D1%80%D1%8B%20%D0%B4%D0%BB%D1%8F&page=46\"
        * string \$comment Комментарий, который подписываем к найденным товарам
        
    Примеры запроса:
        php yii parse-ozon-on-categories 'https://www.ozon.ru/catalog/igrushki/igrovaya-mebel' 'Игровая мебель'
        php yii parse-ozon-on-categories 'https://www.ozon.ru/catalog/0/search.aspx?subject=195&search=%D0%B4%D0%B8%D0%B2%D0%B0%D0%BD&sort=popular' 'диван'
        php yii parse-ozon-on-categories 'https://www.ozon.ru/catalog/0/search.aspx?search=1&sort=priceup'

";
    }

    public function actionIndex($url = "https://www.ozon.ru/category/telefony-i-smart-chasy-15501/", $comment = "", $products_limit=500){
        $this->log_fileName = Yii::getAlias("@root_folder")."/logs/cron/ParseOzonOnCategories_".str_replace(" ", "_", $comment).".txt";

        if ($this->use_lock_file) {
            @mkdir(Yii::getAlias("@root_folder") . "/logs/parse");
            $this->lock_file = fopen(Yii::getAlias("@root_folder") . "/logs/parse/ozon__" . crc32($url) . ".txt", 'w+');
            if (@flock($this->lock_file, LOCK_EX | LOCK_NB)) {//необходимо что бы уменьшить нагрузку на сервак. Парсинг одной и той же страницы не будет выполнятся одновременно более чем 1 раз
                fwrite($this->lock_file, date("Y-m-d H:i:s"));
            } else {
                echo date("Y-m-d H:i:s") . " Парсинг этой страницы уже выполняется\n";
                return;
            }
        }
        $this->date_start = time();
        $this->get_next_proxy();
        $this->log_to_file("start\n\n\n");
        $page = 1;
        $parsePage = $url;
        if (strstr($url, "?") === false) {
            $url .= "?p=";//что бы потом страницу добавлять
        } else {//уже есть какие-то запросы
            preg_match("/&p=\\d*/", $url, $matches);
            if (count($matches) > 0) {
                $page = (int)str_replace("&p=", "", $matches[0]);
                $url = str_replace($matches[0], "", $url);
                $url .= "&p=";
            } else {
                preg_match("/\\?p=\\d*/", $url, $matches);
                if (count($matches) > 0) {
                    $page = (int)str_replace("?p=", "", $matches[0]);
                    $url = str_replace($matches[0], "", $url);
                    $url .= "?p=";
                } else {
                    $url .= "&p=";//что бы потом страницу добавлять
                }
            }
        }
        if ($comment == "") {
            $parse_url = parse_url($url);
            parse_str($parse_url["query"], $get);
            if (isset($get["text"])) {
                $comment = $get["text"];
            }
        }

        $count = 1;
        $min_products_on_page = 10;
        do {
            $current_products_count = $count;
            $html = $this->get_parse_object($url.$page++, 50000);
            $number_of_products_per_page = 0;
            if (!is_null($html)) {
//           foreach ($html->find("script") as $script) {

                $jdata = json_decode($html->find("div[data-state^='{\"items\":[{\"type\"']", 0)->attr["data-state"]);
//             if (($jdata) and (property_exists($jdata, "items") and (property_exists($jdata->items[0], "deepLink")))) {
//                   echo "условие есть ли items и deeplink в jdata истинно";
                foreach ($jdata->items as $item) {
                    $number_of_products_per_page++;
                    try {
                        echo $count.") ";
                        $href = "https://www.ozon.ru" . $item->link;
                        $success = $this->parseProduct($href, $parsePage, $comment);
                        if ($success) $count++;
                    } catch (\Exception $e) {
                        echo 'Ошибка '.$e;
                        $this->log_to_file($e->getMessage());
                    }
                }
                break;
//               }
//           }
                echo "С ".($page - 1)." страницы спарсили ".($count - $current_products_count)." товаров из ".$number_of_products_per_page."\n";
                if ($number_of_products_per_page < $min_products_on_page) {
                    $this->log_to_file("Все вынесли! Уходим!");
                    $this->stop = true;
                }
                if ($count >= $products_limit) {
                    $this->log_to_file("Достигли лимита($products_limit) товаров по запросу! Уходим!");
                    $this->stop = true;
                }
                if (!$this->stop) {
                    $this->log_to_file("Переходим на следующую страницу " . $url . $page . "\n");
                }
            } else {
                $this->log_to_file("Парсинг не удался, закончились прокси");
            }
        } while (!$this->stop);
        $this->log_to_file("Все спарсили".$this->stop);
        $this::actionLastWeekData($parsePage);
        $this->log_to_file("finish\n\n\n");
        if ($this->use_lock_file) fclose($this->lock_file);
    }

    public function actionChangePriceAgainstCompetitors($user_id) {
        if ($this->use_lock_file) {
            @mkdir(Yii::getAlias("@root_folder") . "/logs/parse");
            $this->lock_file = fopen(Yii::getAlias("@root_folder") . "/logs/parse/ozon_competitors_products_" . $user_id . ".txt", 'w+');
            if (@flock($this->lock_file, LOCK_EX | LOCK_NB)) {
                fwrite($this->lock_file, date("Y-m-d H:i:s"));
            } else {
                echo date("Y-m-d H:i:s") . " Парсинг для этого пользователя уже выполняется уже выполняется\n";
                return;
            }
        }
        $this->get_next_proxy();


        $products_ids = OzonCompetitorProduct::find()
            ->distinct()
            ->select("product_id")
            ->where(["user_id" => $user_id])
            ->orderBy(["product_id" => SORT_ASC])
            ->column();


        foreach($products_ids as $product_id) {
            $this->actionChangePriceAgainstCompetitorsOnProduct($user_id, $product_id);
        }

        if ($this->use_lock_file) {
            fclose($this->lock_file);
        }
    }

    public function actionChangePriceAgainstCompetitorsOnProduct($user_id, $product_id) {
        $competitors_products = OzonCompetitorProduct::find()->where(["user_id" => $user_id, "product_id" => $product_id])->all();

        if ($this->cur_proxy_id == -1) {//если запуск был напрямую из терминала
            $this->get_next_proxy();
        }

        $product = OzonProduct::findOne(["user_id" => $user_id, "product_id" => $product_id]);

        if ((empty($product)) or ($product->max_price == 0) or ($product->min_price == 0)) {
            $this->log_to_file("У товара {$product->product_id} не указаны минимальная/максимальная цена");
            return;
        }

        if (!$product->use_price_from_competitor) {
            $this->log_to_file("Товару {$product->product_id} запрещено изменение цены");
            return;
        }
        $current_price_relative_to_competitor = $product->max_price;
        foreach($competitors_products as $competitors_product) {
//            $html = $this->get_parse_object("https://www.ozon.ru/api/composer-api.bx/page/json/spa?url=".urlencode("/context/detail/id/{$competitors_product->ozon_competitor_product_id}/?layout_container=pdpPage2column&layout_page_index=2"), 1000, 'HTML');
            $html = $this->get_parse_object("https://www.ozon.ru/context/detail/id/{$competitors_product->ozon_competitor_product_id}/", 1000, 'HTML', "https://www.google.com/search?q=www.ozon.ru+{$competitors_product->ozon_competitor_product_id}");
            //state-webProductMainWidget
            $price = false;
            $shd = SHD::str_get_html($html);
            if ($div = $shd->find('div[id^="state-webProductMainWidget"]', 0)) {
                $data = json_decode(htmlspecialchars_decode($div->attr["data-state"]));
                if ($data and property_exists($data, "cellTrackingInfo") and property_exists($data->cellTrackingInfo, "product") and property_exists($data->cellTrackingInfo->product, "stockCount") and property_exists($data->cellTrackingInfo->product, "finalPrice")) {
                    if ($data->cellTrackingInfo->product->stockCount > 0) {
                        $price = $data->cellTrackingInfo->product->finalPrice;
                    } else {
                        $price = PHP_INT_MAX;
                    }
                }
            }
            if ($price === false) {
                $pattern_for_searching_price_is_not_zero = "/:{$competitors_product->ozon_competitor_product_id}.*?\"finalPrice\":[1-9]\d*/";
                if (preg_match($pattern_for_searching_price_is_not_zero, $html, $matches)) {
                    $price = (double)preg_replace("/:{$competitors_product->ozon_competitor_product_id}.*?\"finalPrice\":/", "", $matches[0]);
                }
            }
            if (is_bool($price)) {
                $this->log_to_file("Не удалось найти поле с информацией о товаре {$competitors_product->ozon_competitor_product_id} внутри поля 'widgetStates'");
                continue;
            }
            $this->log_to_file("Цена конкурента: {$price}");
            $price -= (double)$competitors_product->discount;//Вычли скидку
            $this->log_to_file("Вычли скидку: {$competitors_product->discount}");
            if ($current_price_relative_to_competitor > $price) {
                if ($price > (double)$product->min_price) {
                    $this->log_to_file("Берём цену конкурента с вычетом скидки: {$price}");
                    $current_price_relative_to_competitor = $price;
                } else {
                    $this->log_to_file("Берём минимальную цену: {$product->min_price}");
                    $current_price_relative_to_competitor = (double)$product->min_price;
                }
            } else {
                $this->log_to_file("{$current_price_relative_to_competitor} <= {$price}, что означает цена текущего товара больше чем та, что мы уже взяли с других товаров");
            }
        }

        $this->log_to_file("Проверяем условие ((({$current_price_relative_to_competitor} <= {$product->max_price}) or (({$product->max_price} == 0)) and ({$current_price_relative_to_competitor} != {$product->main_price})");
        if ((($current_price_relative_to_competitor <= (double)$product->max_price) or ($product->max_price == 0)) and ($current_price_relative_to_competitor != $product->main_price)) {
            $product->main_price = $current_price_relative_to_competitor;

            if (!$product->save(false)) {
                print_r($product->getErrors());
            }
        }
    }

    private function parseProduct($href, $parsePage, $comment) {

        $product = OzonParseOnCategories::findOne(["link" => $href]);
        if (is_null($product)) {
            $this->log_to_file("Создаем новую запись");
            $product = new OzonParseOnCategories();
        }
        $product->link = $href;
        $product->comment = $comment;
        $product->parse_url = $parsePage;
        $product_info = $this->get_parse_object($href, 60000);
        if (!is_null($product_info)) {
//            file_put_contents(Yii::getAlias("@root_folder")."/logs/parse_ozon_test.html", $product_info->innertext);
            if ($model = $product_info->find("h1", 0)) {
                $product->model = trim($model->innertext);
            }

// выдаёт ошибку
//            foreach ($product_info->find("button div div") as $div) {
//                if ($div->innertext == "Добавить в корзину") {
//                    $spans = $div->parent->parent->parent->parent->parent->parent->parent->parent->parent->parent->find("div div span");
//                    echo "Цена товара ".$spans[0].", Старая цена ".$spans[0];
//                    $product->price = (string)$this::customToInt($spans[0]->innertext);
//                    if (count($spans) > 1) {
//                        $product->old_price = (string)$this::customToInt($spans[1]->innertext);
//                    }
//                    break;
//                }
//            }


            $jdata = json_decode($product_info->find("div[id^=\"state-webAddToCart\"]", 0)->attr["data-state"]);
            if ($jdata){
                $jdata = json_decode($product_info->find("div[id^=\"state-webAddToCart\"]", 0)->attr["data-state"]);
                $product->old_price = str_replace(' ₽', '', $jdata->cellTrackingInfo->product->price);
                $product->price = str_replace(' ₽', '', $jdata->cellTrackingInfo->product->finalPrice);
            }

            $product->comments_count = 0;
            $product->product_rating = 0;

            $jdata = json_decode($product_info->find("script[data-n-head]",0)->innertext);
            ($jdata);
            if ($jdata){
                $product->product_rating = $jdata->aggregateRating->ratingValue;
                $product->comments_count = $jdata->aggregateRating->reviewCount;
            }

// Выдаёт ошибку
//            if ($comments_count = $product_info->find("a[href^='/reviews/']", 0)) {
//                $product->comments_count = $this::customToInt($comments_count->innertext);
//                if ($div = $comments_count->parent->find("div", 0)) {
//                    $product->product_rating = trim($div->attr["title"]);
//                }
//            }




            if (($span = $product_info->find("span[title='Наиболее популярный, хорошо продающийся на OZON.ru товар']", 0)) and ($span->innertext == "Бестселлер")) {
                $product->bestseller = 1;
            } else {
                $product->bestseller = 0;
            }

            foreach ($product_info->find("div") as $div) {
                if ($div->innertext == "Продавец:") {
                    $seller = $div->parent->find("div a", 0);
                    if ($seller) {
                        $product->seller_name = $seller->innertext;
                        $product->seller_link = $seller->attr["href"];
                    } else {
                        $product->seller_name = "OZON";
                        $product->seller_link = "https://www.ozon.ru";
                    }
                    break;
                }
            }

            foreach ($product_info->find("a[href^='#']") as $a) {
                if ($a->innertext == "Перейти к описанию") {
                    $div = $a->parent->parent->parent;
                    if (count($div->children) == 1) {
                        $div = $div->parent;
                    }
                    $div = end($div->children);
                    if ($div) {
                        if (($a = $div->find("a", 0)) and (stripos($a->attr["href"], "https://") !== false)) {
                            $product->brand_link = $a->attr["href"];
                            if (empty($product->brand_link)) $product->brand_link = "";
                            if ($img = $div->find("img", 0)) {
                                $product->brand_logo = $img->attr["src"];
                            }
                            if (empty($product->brand_logo)) $product->brand_logo = "";
                        }
                    }
                    break;
                }
            }

            $full_category_path = [];
            foreach ($product_info->find("ol li meta[content^='']") as $meta) {
                if ($category = $meta->parent->find("a span", 0)) {
                    $full_category_path[] = $category->innertext;
                } elseif ($category = $meta->parent->find("span span", 0)) {
                    $full_category_path[] = $category->innertext;
                }
            }
            $product->full_category = implode("/", $full_category_path);

            if ($min_img = $product_info->find("div[data-index^=''] div img", 0)) {
                $product->picture = $min_img->parent->parent->parent->parent->parent->parent->children[1]->find("img", 0)->attr["src"];
                if (empty($product->picture)) {
                    $product->picture = $min_img->parent->parent->parent->parent->parent->parent->parent->children[1]->find("img", 0)->attr["src"];
                }
            } else {
                foreach ($product_info->find("img") as $img) {
                    if ((count($img->parent->children) == 2) and ($img->parent->children[1]->tag == "div") and ($img->parent->children[1]->innertext == "")) {
                        $product->picture = $img->attr["src"];
                        break;
                    }
                }
            }


            if ($this->stop) {
                $this->log_to_file("Закончились прокси");
            }

            (new DynamicsOzon(["link" => $product->link, "price" => $product->price, "comments_count" => $product->comments_count, "bestseller" => $product->bestseller]))->save();
            if ($product->save()) {
                $this->log_to_file("Сохранили запись по товару " . $href . "\n");
                return true;
            } elseif($product->save(false)) {
                $this->log_to_file("Сохранили запись по товару " . $href . "\n");
                return true;
            } else {
                $this->log_to_file(print_r($product->getErrors(), true));
                return false;
            }
        }
        return false;
    }

    public function get_next_proxy() {
        $this->cur_proxy_id++;
        $field = "ozon_weight";
        $select = new \yii\db\Expression("`id`, `ip`, `$field` as `weight`");
        $order_by = new \yii\db\Expression("`$field` DESC, `proxy_ip`.`weight` DESC, `aliexpress_weight` DESC, `youla_weight` DESC");

        $proxy_list = Proxy::find()->select($select)->where(['>', $field, -5])->orderBy($order_by)->limit(10)->all();

        $this->max_proxy_id = Proxy::find()->where(['>', $field, -5])->count();
        if ((count($proxy_list) == 0) or ($this->cur_proxy_id >= $this->max_proxy_id)) {
            $this->log_to_file("Закончились прокси, запросим новые");
//            ProxyController::actionIndex();
            ProxyController::actionIndex("https://hidemyna.me/ru/api/proxylist.php?out=js&country=RU&type=h&code=280792319681495", "ozon_weight");

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
    }

    public static function customToInt($str) {
        return (int)preg_replace("/[^\\d]/", "", $str);
    }

    function get_parse_object($url, $min_page_length=200000, $format="SHD", $referer=""){
//        sleep(rand(2, 4));
        $timeout = 40;
        $max_redirects = 5;
        while($this->cur_proxy_id < $this->max_proxy_id) {
            sleep(rand(2, 5));
            $this->log_to_file("Запрашиваем страницу: $url ".(($this->use_proxy)?" (".$this->str_proxy->ip."[".$this->str_proxy->weight."])":""));
            if ($ch = curl_init()) {
//                $url_data = parse_url($url);
                $headers = [
                    ':authority: www.ozon.ru', 
                    ':method: GET', 
//                    ':path: ' . $url_data["path"] . "?" . $url_data["query"], 
                    ':scheme: https', 
                    'accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 
//                    'accept-encoding: gzip, deflate, br',
                    'accept-language: ru,en;q=0.9', 
                    'cache-control: no-cache', 
                    'pragma: no-cache', 
                    'upgrade-insecure-requests: 1', 
                    'connection: keep-alive',
                ];
                curl_setopt($ch, CURLOPT_HEADER, 1);
                curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
                curl_setopt($ch, CURLOPT_USERAGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36");
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
                curl_setopt($ch, CURLOPT_URL, $url);
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
                curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
                $cookie = implode(";", array_map(function($key, $value) { return "{$key}={$value}"; }, array_keys($this->cookie), $this->cookie));
                curl_setopt($ch, CURLOPT_COOKIE, $cookie);
                curl_setopt($ch, CURLOPT_TIMEOUT, $timeout);
                if ($referer !== "") {
                    curl_setopt($ch, CURLOPT_REFERER, $referer);
                }
                curl_setopt($ch, CURLOPT_MAXREDIRS, $max_redirects);
                if ($this->use_proxy) {
                    curl_setopt($ch, CURLOPT_PROXY, $this->str_proxy->ip);
                    curl_setopt($ch, CURLOPT_PROXYTYPE, CURLPROXY_HTTP);
                }
                $html = curl_exec($ch);
                $errmsg = curl_error($ch);
                $header_size = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
                curl_close($ch);
                $response_headers = substr($html, 0, $header_size);
                $html = substr($html, $header_size);

//                $cookie_pattern = "/^Set-Cookie:\s[^;]+/m";//кастом для занесения кук
//                preg_match_all($cookie_pattern, $response_headers, $matches);
//                $cookies_keys = ["visid_incap", "incap_ses", "tmr_reqNum"];
//                foreach($matches[0] as $header) {
//                    $set_cookie = explode("=", preg_replace("/Set-Cookie:\s/", "", $header));
//                    $cookie_name = array_shift($set_cookie);
//                    foreach($cookies_keys as $cookies_key) {
//                        if (stripos($cookie_name, $cookies_key) === 0) {
//                            $current_key = preg_grep("/{$cookies_key}/", array_keys($this->cookie));
//                            if (count($current_key) > 0) {
//                                unset($this->cookie[array_first($current_key)]);
//                            }
//                            $this->cookie[$cookie_name] = implode("=", $set_cookie);
//                            break;
//                        }
//                    }
//                }

                $this->log_to_file("Длинна полученной страницы " . strlen($html) . "\n");

                if ($html == "" and $errmsg == "") return false;
                if (strlen($html) < $min_page_length) {
                    if (!$this->use_proxy) {
                        $this->log_to_file("Включили прокси");
                        $this->use_proxy = true;
                        continue;
                    }
                    sleep(rand(2, 6));

                    Proxy::updateAll(["ozon_weight" => $this->str_proxy["weight"] - 1], ["id" => $this->str_proxy["id"]]);

                    $this->get_next_proxy();

                    if ($this->cur_proxy_id >= $this->max_proxy_id) {//костыль от старых кодов, начинаем с начала
                        $this->cur_proxy_id = 0;
                    }
                    continue;
                } else {
                    if (($this->use_proxy) and ($this->str_proxy["weight"] < 10)) {
                        Proxy::updateAll(["ozon_weight" => $this->str_proxy["weight"] + 1], ["id" => $this->str_proxy["id"]]);
                    }
                }
                $this->cookie["tmr_reqNum"] = (string)++$this->cookie["tmr_reqNum"];

                if ($format == "SHD") {
                    $html = SHD::str_get_html($html);
                }


                return $html;

            } else {
                $this->log_to_file("curl no init");


                return null;
            }
        }
        $this->log_to_file("Закончились прокси");
        $this->stop = true;


        return null;
    }

    public static function actionLastWeekData($parse_url, $echo=false) {
        $limit = 5000;
        $offset = 0;
        $counter = 0;
        do {
            $products = OzonParseOnCategories::find()->where(["parse_url" => $parse_url])->limit($limit)->offset($offset)->all();
            $offset += $limit;
            $interval = 15;
            $date = date("Y-m-d H:i:s", strtotime("-" . $interval . " day"));
            foreach ($products as $product) {
                $dynamics = DynamicsOzon::find()
                    ->select("((SELECT `comments_count` FROM `dynamics_ozon` WHERE `link`=\"" . $product->link . "\" and `date`>\"" . $date . "\" ORDER BY `date` DESC LIMIT 1) - (SELECT `comments_count` FROM `dynamics_ozon` WHERE `link`=\"" . $product->link . "\" and `date`>\"" . $date . "\" ORDER BY `date` ASC LIMIT 1)) as `recent_comments`")
                    ->where(["link" => $product->link])
                    ->andWhere([">", "date", $date])
                    ->asArray()
                    ->one();

                //$product->recent_comments = $dynamics["recent_comments"];
                if ($product->save() and $echo) {
                    echo $counter++.") Save " . $product->link . "\n";
                }
            }
        } while (count($products) == $limit);
    }

    public function log_to_file($str, $date=1){
        echo (($date)?date("Y-m-d H:i:s") . " ":"") . "$str \n";
        //        file_put_contents($this->log_fileName, (($date) ? date("Y-m-d H:i:s") : "") . "(" . $this->date_start . ") $str \n");
    }
}