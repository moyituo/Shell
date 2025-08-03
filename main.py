from converter.success_file_converter import SuccessConverter

if __name__ == '__main__':
    converter = SuccessConverter()
    converter.convert()

    # origin_file_info_path='http://10.10.3.13:18082/group1/originalData/zgq_simulation_test/????????/3885/Hozon_EP12_0361_Tianjin_20240119150445049000.vsb'
    # # name_value = str.split('name=')[1].split('&')[0]
    # # print(name_value)
    #
    # origin_file_info_path = origin_file_info_path.replace(
    #     'http://10.10.3.13:18082/group1/originalData/', '')
    # origin_file_info_path = "bucket" + '/' + '/'.join(
    #     origin_file_info_path.split('/')[:-1])
    # print(origin_file_info_path)
