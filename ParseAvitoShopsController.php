<?php

namespace console\controllers;

use yii;
use yii\console\Controller;
use keltstr\simplehtmldom\SimpleHTMLDom as SHD;
use console\models\Proxy;
use backend\models\AvitoParseShops;

/**
 * Feed controller
 */
class ParseAvitoShopsController extends Controller
{

    public $str_proxy;
    public $max_proxy_id;
    public $category = '';
    public $log_fileName = "";
    public $cur_proxy_id = -1;
    public $stop = false;

    public function actionHelp(){
        echo "Url категорий для парсинга:\n";
        echo "  Товары для детей и игрушки: /rossiya/tovary_dlya_detey_i_igrushki\n";
        echo "  Спорт и отдых: /rossiya/sport_i_otdyh\n";
        echo "  Оборудование для бизнеса: /rossiya/oborudovanie_dlya_biznesa\n";
        echo "  Детская одежда и обувь: /rossiya/detskaya_odezhda_i_obuv\n";
        echo "  Готовый бизнес: /rossiya/gotoviy_biznes\n";
        echo "  Мебель и интерьер: /rossiya/mebel_i_interer\n";
        echo "  Посуда и товары для кухни: /rossiya/posuda_i_tovary_dlya_kuhni\n";
        echo "  Ремонт и строительство: /rossiya/remont_i_stroitelstvo\n";
        echo "  Бытовая электроника: /rossiya/bytovaya_elektronika\n";
        echo "  Бытовая техника: /rossiya/bytovaya_tehnika\n";
        echo "  Одежда, обувь, аксессуары: /odezhda_obuv_aksessuary\n";
        echo "  Запчасти и аксессуары: /rossiya/zapchasti_i_aksessuary\n";
    }

    private function get_next_proxy() {
        $this->cur_proxy_id++;

        $proxy_list = Proxy::find()->where(['>', 'weight', -5])->orderBy("weight DESC")->all();

        sleep(rand(2, 4));

        $this->max_proxy_id = count($proxy_list) - 1;
        if ((count($proxy_list) == 0) or ($this->cur_proxy_id >= $this->max_proxy_id)) {
            $this->log_to_file("Закончились прокси, запросим новые");
            ProxyController::actionIndex();

            $this->cur_proxy_id = 0;

            $proxy_list = Proxy::find()->where(['>', 'weight', -5])->orderBy("weight DESC")->all();

            $this->max_proxy_id = count($proxy_list) - 1;
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
            if (count($proxy_list) > 10) {
                $this->str_proxy = $proxy_list[rand(0,9)];
            } else {
                $this->str_proxy = $proxy_list[rand(0, count($proxy_list) - 1)];
            }
        }

        echo "\n" . date("Y-m-d-H-i-s") . " Переключаем прокси на " . $this->str_proxy->id . ":" . $this->str_proxy->ip . " \n";

    }

    private function get_parse_object($url, $maximum_page_length=50000){
        $wd = Yii::getAlias("@root_folder");

        sleep(rand(2, 4));
        while($this->cur_proxy_id < $this->max_proxy_id) {
            sleep(1);
            $this->log_to_file("Запрашиваем страницу: $url ");
            echo date("Y-m-d H:i:s") ." Используем прокси ".$this->str_proxy->ip."\n";
            if ($ch = curl_init()) {

                curl_setopt($ch, CURLOPT_URL, $url);
                curl_setopt($ch, CURLOPT_HEADER, false);
                curl_setopt($ch, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0");
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                curl_setopt($ch, CURLOPT_AUTOREFERER, true);
                curl_setopt($ch, CURLOPT_COOKIESESSION, true);
                curl_setopt($ch, CURLOPT_FAILONERROR, true);
                curl_setopt($ch, CURLOPT_TIMEOUT, 30); // http request timeout 20 seconds
                curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true); // Follow redirects, need this if the url changes
                curl_setopt($ch, CURLOPT_MAXREDIRS, 3); //if http server gives redirection responce
                curl_setopt($ch, CURLOPT_COOKIEJAR, "$wd/logs/cookies/avito_parse_cookies.txt"); // cookies storage / here the changes have been made
                curl_setopt($ch, CURLOPT_COOKIEFILE, "$wd/logs/cookies/avito_parse_cookies.txt");
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, true); // false for https

                curl_setopt($ch, CURLOPT_PROXY, $this->str_proxy['ip']);
                curl_setopt($ch, CURLOPT_PROXYTYPE, CURLPROXY_HTTP);

                $html = curl_exec($ch);
                $errmsg  = curl_error($ch);
                curl_close($ch);
                $this->log_to_file("Длинна полученной страницы " . strlen($html) . "\n");

                if (strlen($html) < $maximum_page_length) {
                    sleep(rand(2, 4));

                    Proxy::updateAll(["weight" => $this->str_proxy["weight"] - 1], ["id" => $this->str_proxy["id"]]);
                    echo date("Y-m-d H:i:s") . " Banned proxy: ".$this->str_proxy->id . " \n";

                    // этот прокси забанили - переключимся на новый и продолжим
                    $this->get_next_proxy();
                    if ($this->cur_proxy_id >= $this->max_proxy_id){
                        $this->cur_proxy_id = 0;
                        echo "\n" . date("Y-m-d H:i:s") . " Переключаем прокси на $this->cur_proxy_id - сначала \n";
                    }
                    continue;
                } else {
                    if ($this->str_proxy["weight"] < 10) {
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

        // прокси больше не осталось - полный бан
        $this->log_to_file("Закончились прокси");
        $this->stop = true;
        
        
        return false;
    }


    public function actionIndex($url_category_shops = '', $query = ""){

        $url_category_shops = trim($url_category_shops);

        if ($url_category_shops == '') {
            $wd = Yii::getAlias("@root_folder");
            $fp = fopen("$wd/parse_shops.txt", "r");
            $number = (int)trim(fgets($fp));
            fclose($fp);
            
            switch ($number) {
                case 0 : $url_category_shops = "/rossiya/tovary_dlya_detey_i_igrushki"; break;
                case 1 : $url_category_shops = "/rossiya/sport_i_otdyh"; break;
                case 2 : $url_category_shops = "/rossiya/oborudovanie_dlya_biznesa"; break;
                case 3 : $url_category_shops = "/rossiya/detskaya_odezhda_i_obuv"; break;
                case 4 : $url_category_shops = "/rossiya/gotoviy_biznes"; break;
                case 5 : $url_category_shops = "/rossiya/mebel_i_interer"; break;
                case 6 : $url_category_shops = "/rossiya/posuda_i_tovary_dlya_kuhni"; break;
                case 7 : $url_category_shops = "/rossiya/remont_i_stroitelstvo"; break;
                case 8 : $url_category_shops = "/rossiya/bytovaya_elektronika"; break;
                case 9 : $url_category_shops = "/rossiya/bytovaya_tehnika"; break;
                case 10 : $url_category_shops = "/rossiya/odezhda_obuv_aksessuary"; break;
                case 11 : $url_category_shops = "/rossiya/zapchasti_i_aksessuary"; break;
            }
            $fp = fopen("$wd/parse_shops.txt", "w");
            if ($number == 11) fwrite($fp, 0);
            else fwrite($fp, $number+1);
            fwrite($fp, "\n".date('Y-m-d H:i:s'));
            fclose($fp);
        }



        switch ($url_category_shops){
            case "/rossiya/tovary_dlya_detey_i_igrushki": {$this->category = "Товары для детей и игрушки";} break;
            case "/rossiya/sport_i_otdyh"               : {$this->category = "Спорт и отдых"             ;} break;
            case "/rossiya/oborudovanie_dlya_biznesa"   : {$this->category = "Оборудование для бизнеса"  ;} break;
            case "/rossiya/detskaya_odezhda_i_obuv"     : {$this->category = "Детская одежда и обувь"    ;} break;
            case "/rossiya/gotoviy_biznes"              : {$this->category = "Готовый бизнес"            ;} break;
            case "/rossiya/mebel_i_interer"             : {$this->category = "Мебель и интерьер"         ;} break;
            case "/rossiya/posuda_i_tovary_dlya_kuhni"  : {$this->category = "Посуда и товары для кухни" ;} break;
            case "/rossiya/remont_i_stroitelstvo"       : {$this->category = "Ремонт и строительство"    ;} break;
            case "/rossiya/bytovaya_elektronika"        : {$this->category = "Бытовая электроника"       ;} break;
            case "/rossiya/bytovaya_tehnika"            : {$this->category = "Бытовая техника"           ;} break;
            case "/rossiya/odezhda_obuv_aksessuary"     : {$this->category = "Одежда, обувь, аксессуары" ;} break;
            case "/rossiya/zapchasti_i_aksessuary"      : {$this->category = "Запчасти и аксессуары"     ;} break;
        }

        $wd = Yii::getAlias("@root_folder");

        $this->log_fileName = $wd."/logs/parse_avito_shops/".substr($url_category_shops, strripos($url_category_shops, "/") + 1).(($query == "")?"":"_".$query).".txt";
        echo "Логируем в файл: ".$this->log_fileName."\n";

        if ($this->category != '') {
            AvitoParseShops::updateAll(["shop_relevance" => 0], ['category' => $this->category]);
        }
        $new = 0;
        $i = 0;
        $this->get_next_proxy();
        $page_index = 0;
        $max_page_index = 99999;
        $count_shops = 0;
        $ban = 0;
        $current_shop = 0;

        while ($page_index < $max_page_index){
            $page_index++;
            $ban = 0;
            $i++;

            $url = "https://www.avito.ru/shops$url_category_shops?p=$page_index".(($query == "")?"":"&q=".urlencode($query));
            $this->log_to_file("\n\nПереходим на следующую страницу ".$url."\n\n");

            if ($ch = curl_init()) {

                curl_setopt($ch, CURLOPT_URL, $url);
                curl_setopt($ch, CURLOPT_HEADER, false);
                curl_setopt($ch, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0");
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                curl_setopt($ch, CURLOPT_AUTOREFERER, true);
                curl_setopt($ch, CURLOPT_COOKIESESSION, true);
                curl_setopt($ch, CURLOPT_FAILONERROR, true);
                curl_setopt($ch, CURLOPT_TIMEOUT, 30); // http request timeout 20 seconds
                curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true); // Follow redirects, need this if the url changes
                curl_setopt($ch, CURLOPT_MAXREDIRS, 3); //if http server gives redirection responce
                curl_setopt($ch, CURLOPT_COOKIEJAR, "$wd/logs/cookies/avito_parse_cookies.txt"); // cookies storage / here the changes have been made
                curl_setopt($ch, CURLOPT_COOKIEFILE, "$wd/logs/cookies/avito_parse_cookies.txt");
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, true); // false for https

                curl_setopt($ch, CURLOPT_PROXY, $this->str_proxy['ip']);
                curl_setopt($ch, CURLOPT_PROXYTYPE, CURLPROXY_HTTP);

                $html = curl_exec($ch);

                if (strlen($html) == 0) {
                    sleep(rand(1, 3));
                    $tl = date("Y-m-d-H-i-s") . " " . curl_error($ch) . " = " . curl_errno($ch) . "\n";
                    $tl .= $url . "\n";
                    file_put_contents("$wd/logs/parse.txt", $tl, LOCK_EX + FILE_APPEND);
                    $this->log_to_file("$tl");
                    curl_close($ch);

                    $this->str_proxy["weight"] -= 1;
                    $this->str_proxy->save();
                    $this->get_next_proxy();
                    $page_index--;
                    continue;
                }

                curl_close($ch);

                if (strlen($html) < 10000) {
                    $this->str_proxy["weight"] -= 1;
                    $this->str_proxy->save();
                    $this->get_next_proxy();
                    $page_index--;
                    file_put_contents("$wd/logs/avito_parse_ban.txt", date("Y-m-d-H-i-s") . " strlen(html)=" . strlen($html) . " \n", FILE_APPEND);
                    file_put_contents("$wd/logs/avito_parse_shops/cat-" . date("Y-m-d-H-i-s") . "-$i-ban.html", $html);
                    continue;
                }

            } else {

                $tl = "catalog no curl\n";
                $tl .= $url . "\n\n";
                file_put_contents("$wd/logs/avito_parse_log.txt", $tl, LOCK_EX + FILE_APPEND);
                continue;

            }

            $html = SHD::str_get_html($html);
            if ($this->str_proxy["weight"] < 10) {
                $this->str_proxy["weight"] += 1;
                $this->str_proxy->save();
            }

            if ($max_page_index == 99999) {
                foreach ($html->find("div.pagination-pages a") as $page_link) {
                    $last_page_href = $page_link->attr["href"];
                    preg_match("/p\=(\d+)/", $last_page_href, $max_page_match);
                    $max_page_index = $max_page_match[1];
                }
                $this->log_to_file("Последняя страница " . $max_page_index);

                if (count($html->find("span.breadcrumbs-link-count")) > 0) {
                    $this->log_to_file("Количество магазинов " . trim($html->find("span.breadcrumbs-link-count", 0)->plaintext));
                } else {
                    $this->log_to_file("Без понятия сколько всего магазинов");
                }
            }

            if ((count($html->find('div.t_s_i')) < 40) and ($page_index != $max_page_index)) {
                $this->log_to_file("На странице меньше 40 магазинов и это не последняя страница, давай по новой");
                $page_index--;
                continue;
            } else {
                $this->log_to_file("На странице представлено ".count($html->find('div.t_s_i'))." магазинов");
            }

            foreach ($html->find('div.t_s_i') as $shop) {
                $this->log_to_file(++$current_shop.") ");
                try {
                    
                    $city_and_products_count = trim($shop->find('div.t_s_items', 0)->plaintext);
                    $city_and_products_count = html_entity_decode($city_and_products_count);

                    $item['name'] = trim($shop->find('h3 a', 0)->plaintext);
                    $item['name'] = html_entity_decode($item['name']);

                    if (count($shop->find('div.t_s_categories')) > 0) {
                        $item['category'] = trim($shop->find('div.t_s_categories', 0)->plaintext);
                    } else {
                        $item['category'] = "";
                    }
                    $item['city'] = trim(preg_replace('/(\d+).+/i', '', $city_and_products_count));
                    $item['url'] = str_replace("?page_from=from_shops_list", "", "https://www.avito.ru" . trim($shop->find('h3 a', 0)->attr["href"]));
                    $item['logo_url'] = trim($shop->find('img.t_s_photo_img', 0)->attr["src"]);

                    $item['products_count'] = preg_replace('/^([^\d+])+/i', '', $city_and_products_count);
                    $item['products_count'] = (int)preg_replace('/\s+/i', '', $item['products_count']);

                    $item['description'] = trim($shop->find('em.t_s_em', 0)->plaintext);
                    $item['description'] = html_entity_decode($item['description']);

                    $html_shop = $this->get_parse_object($item['url']);
                    if ($html_shop != "") {
                        if (count($html_shop->find('div.shop-header-shop-header-phone-3Ivio')) > 0) {
                            $item['phone'] = str_replace("-", "", str_replace(" ", "", trim($html_shop->find('div.shop-header-shop-header-phone-3Ivio', 0)->innertext)));
                        } else {
                            $item['phone'] = "";
                        }

                        foreach ($item as &$it) {
                            $it = trim($it);
                            $it = addslashes($it);
                        }

                        $shop = AvitoParseShops::find()
                            ->where(['url' => $item['url']])
                            ->one();

                        if (isset($shop)) {
                            $shop['name'] = $item['name'];
                            $shop['city'] = $item['city'];
                            $shop['logo_url'] = $item['logo_url'];
                            $shop['products_count'] = $item['products_count'];
                            $shop['description'] = $item['description'];
                            $shop['category'] = $this->category;
                            $shop['phone'] = $item['phone'];
                            $shop['shop_relevance'] = 1;
                            if ($query != "") {
                                $shop['metka'] = $query;
                            }
                            if (!$shop->save(false)) $this->log_to_file("Ошибка сохранения");
                        } else {
                            $shop = new AvitoParseShops();
                            $shop['name'] = $item['name'];
                            $shop['city'] = $item['city'];
                            $shop['url'] = $item['url'];
                            $shop['logo_url'] = $item['logo_url'];
                            $shop['products_count'] = $item['products_count'];
                            $shop['description'] = $item['description'];
                            $shop['category'] = $this->category;
                            $shop['phone'] = $item['phone'];
                            $shop['shop_relevance'] = 1;
                            $shop['cdate'] = date('Y-m-d');
                            if ($query != "") {
                                $shop['metka'] = $query;
                            }
                            if ($shop->save(false)) {
                                $new++;

                                Yii::$app->mailer->compose([$shop])
                                    ->setFrom('report@butuz.club')
                                    ->setTo('andrey@alisa.market')
                                    ->setSubject("Новый магазин на Авито!")
                                    ->setTextBody("Категория:" . $shop['category'] . ".\n
                            Название:" . $shop['name'] . "\n
                            Город:" . $shop['city'] . "\n
                            Объявлений:" . $shop['products_count'] . "\n
                            Ссылка:" . $shop['url'] . "\n
                            Телефон:" . $shop['phone'] . "\n
                            Описание:" . $shop['description'] . "\n")
                                    ->send();
                            } else $this->log_to_file("Ошибка сохранения");
                        }
                    } else {
                        $this->log_to_file("Не смогли спарсить магазин " . $item['url']);
                    }
                    $count_shops++;
                } catch (\Exception $e) {
                    $this->log_to_file($e->getMessage());
                }
            }
            sleep(2);
        }

        $this->log_to_file("Всего магазинов обработали: $count_shops ", 1);
        $this->log_to_file("Новых магазинов: $new ", 1);

        if ($ban) {
            $this->log_to_file("////BAN////", 1);
        }

    }

    public function log_to_file($str, $date=1){
        echo (($date)?date("Y-m-d H:i:s") . " ":"") . "$str \n";
        file_put_contents($this->log_fileName, (($date)?date("Y-m-d H:i:s"):"") . " $str \n", FILE_APPEND | LOCK_EX);
    }
}