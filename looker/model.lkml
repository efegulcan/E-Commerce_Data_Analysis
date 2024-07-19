explore: ECommerceDataset {
  view: ECommerceDataset {
    dimension: order_date {
      type: date
      sql: ${TABLE}.order_date ;;
    }
    measure: total_sales {
      type: sum
      sql: ${TABLE}.sales ;;
    }
  }

  visualization: worldmap {
    type: map
    sql: ${TABLE}.location ;;
    measure: total_sales
  }
}
